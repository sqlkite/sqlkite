package sqlkite

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"src.goblgobl.com/sqlite"
	"src.goblgobl.com/utils"
	"src.goblgobl.com/utils/buffer"
	"src.goblgobl.com/utils/concurrent"
	"src.goblgobl.com/utils/kdr"
	"src.goblgobl.com/utils/log"
	"src.goblgobl.com/utils/validation"
	"src.sqlkite.com/sqlkite/codes"
	"src.sqlkite.com/sqlkite/data"
	"src.sqlkite.com/sqlkite/sql"
	"src.sqlkite.com/sqlkite/super"
)

type BufferType int

const (
	BUFFER_TYPE_GENERATE_SQL BufferType = iota
	BUFFER_TYPE_RESULT
)

var (
	Projects concurrent.Map[*Project]
)

func init() {
	Projects = concurrent.NewMap[*Project](loadProject, shutdownProject)
}

// A project instance isn't updated. If the project is changed,
// a new instance is created.
type Project struct {
	// The pool of SQLite connections to this database. The size of this
	// pool is controlled by the MaxConcurrency value.
	dbPool *DBPool

	tables map[string]*Table

	Id string

	// Any log entry generate for this project should include
	// the pid=$id field
	logField log.Field

	Auth   data.Auth
	Limits data.Limits

	// Project-specific counter for generating the RequestId
	requestId uint32

	// When enabled, error response bodies may include additional data. This data
	// could leak internal implementation details (e.g. table schemas). But having
	// more data available in the response, without having to check a console or
	// central log system, is pretty valuable.
	Debug bool
}

func (p *Project) Shutdown() {
	p.dbPool.shutdown()
}

func (p *Project) Env() *Env {
	return NewEnv(p, p.NextRequestId())
}

func (p *Project) NextRequestId() string {
	nextId := atomic.AddUint32(&p.requestId, 1)
	return utils.EncodeRequestId(nextId, Config.InstanceId)
}

func (p *Project) Table(name string) *Table {
	return p.tables[name]
}

func (p *Project) WithDB(cb func(sqlite.Conn) error) error {
	pool := p.dbPool
	conn := pool.Checkout()
	defer pool.Release(conn)
	return cb(conn)
}

func (p *Project) WithTransaction(cb func(sqlite.Conn) error) error {
	return p.WithDB(func(conn sqlite.Conn) error {
		return conn.Transaction(func() error {
			return cb(conn)
		})
	})
}

func (p *Project) CreateTable(env *Env, table *Table) error {
	if max := p.Limits.MaxTableCount; len(p.tables) >= int(max) {
		env.VC.Invalid(&validation.Invalid{
			Code:  codes.VAL_TOO_MANY_TABLES,
			Error: "Maximum table count reached",
			Data:  validation.MaxData(int(max)),
		})
		return nil
	}

	definition, err := json.Marshal(table)
	if err != nil {
		return log.Err(codes.ERR_TABLE_SERIALIZE, err)
	}

	createBuffer := p.SQLBuffer()
	defer createBuffer.Release()

	table.Write(createBuffer)
	createSQL, err := createBuffer.SqliteBytes()
	if err != nil {
		return p.bufferErrorToValidation(env, err, BUFFER_TYPE_GENERATE_SQL)
	}

	// This is aweful. I really struggle between giving each trigger its own buffer
	// or re-using the createBuffer. The issue with re-using createBuffer is
	// that we'd need to generate the trigger SQL within the transaction, which
	// I don't love for the performance implication and for handling buffer.ErrMaxSize
	// as a validation error within the transaction.
	// Doing this mess upfront at least makes the code that's executing within
	// our transaction pretty straightforward.

	access := table.Access
	tableName := table.Name
	insertAccessSQL, insertAccessBuffer, err := p.buildAccessTrigger(access.Insert)
	if err != nil {
		return p.bufferErrorToValidation(env, err, BUFFER_TYPE_GENERATE_SQL)
	}
	defer insertAccessBuffer.Release()

	updateAccessSQL, udpdateAccessBuffer, err := p.buildAccessTrigger(access.Update)
	if err != nil {
		return p.bufferErrorToValidation(env, err, BUFFER_TYPE_GENERATE_SQL)
	}
	defer udpdateAccessBuffer.Release()

	deleteAccessSQL, deleteAccessBuffer, err := p.buildAccessTrigger(access.Delete)
	if err != nil {
		return p.bufferErrorToValidation(env, err, BUFFER_TYPE_GENERATE_SQL)
	}
	defer deleteAccessBuffer.Release()

	err = p.WithTransaction(func(conn sqlite.Conn) error {
		if err := conn.ExecTerminated(createSQL); err != nil {
			return log.Err(codes.ERR_CREATE_TABLE_EXEC, err).Binary("sql", createSQL)
		}

		if insertAccessSQL != nil {
			if err := conn.ExecTerminated(insertAccessSQL); err != nil {
				return log.Err(codes.ERR_CREATE_ACCESS_TRIGGER, err).String("action", "insert").Binary("sql", insertAccessSQL)
			}
		}

		if updateAccessSQL != nil {
			if err := conn.ExecTerminated(updateAccessSQL); err != nil {
				return log.Err(codes.ERR_CREATE_ACCESS_TRIGGER, err).String("action", "update").Binary("sql", updateAccessSQL)
			}
		}

		if deleteAccessSQL != nil {
			if err := conn.ExecTerminated(deleteAccessSQL); err != nil {
				return log.Err(codes.ERR_CREATE_ACCESS_TRIGGER, err).String("action", "delete").Binary("sql", deleteAccessSQL)
			}
		}

		if err := conn.Exec("insert into sqlkite_tables (name, definition) values ($1, $2)", tableName, definition); err != nil {
			return log.Err(codes.ERR_INSERT_SQLKITE_TABLES, err)
		}

		return nil
	})

	if err != nil {
		return err
	}

	// update our in-memory project
	_, err = ReloadProject(p.Id)
	return err
}

func (p *Project) UpdateTable(env *Env, table *Table, alter TableAlter) error {
	existing := p.tables[alter.Name]
	if existing == nil {
		env.VC.Invalid(UnknownTable(alter.Name))
		return nil
	}

	applyTableChanges(table, existing, alter)

	definition, err := json.Marshal(table)
	if err != nil {
		return log.Err(codes.ERR_TABLE_SERIALIZE, err)
	}

	updateBuffer := p.SQLBuffer()
	defer updateBuffer.Release()

	alter.Write(updateBuffer)
	alterSQL, err := updateBuffer.SqliteBytes()
	if err != nil {
		return p.bufferErrorToValidation(env, err, BUFFER_TYPE_GENERATE_SQL)
	}

	tableName := table.Name
	existingTableName := existing.Name

	existingAccess := existing.Access

	// See comments in CreateTable about my conflicting feelings about how this
	// should be done.
	insertAccessSQL, insertAccessBuffer, err := p.buildAccessTriggerChange(alter.InsertAccess, tableName, existingAccess.Insert, existingTableName)
	if err != nil {
		return p.bufferErrorToValidation(env, err, BUFFER_TYPE_GENERATE_SQL)
	}
	defer insertAccessBuffer.Release()

	updateAccessSQL, udpdateAccessBuffer, err := p.buildAccessTriggerChange(alter.UpdateAccess, tableName, existingAccess.Update, existingTableName)
	if err != nil {
		return p.bufferErrorToValidation(env, err, BUFFER_TYPE_GENERATE_SQL)
	}
	defer udpdateAccessBuffer.Release()

	deleteAccessSQL, deleteAccessBuffer, err := p.buildAccessTriggerChange(alter.DeleteAccess, tableName, existingAccess.Delete, existingTableName)
	if err != nil {
		return p.bufferErrorToValidation(env, err, BUFFER_TYPE_GENERATE_SQL)
	}
	defer deleteAccessBuffer.Release()

	err = p.WithTransaction(func(conn sqlite.Conn) error {
		if err := conn.ExecTerminated(alterSQL); err != nil {
			return log.Err(codes.ERR_UPDATE_TABLE_EXEC, err).Binary("sql", alterSQL)
		}

		if insertAccessSQL != nil {
			if err := conn.ExecTerminated(insertAccessSQL); err != nil {
				return log.Err(codes.ERR_UPDATE_ACCESS_TRIGGER, err).String("action", "insert").Binary("sql", insertAccessSQL)
			}
		}

		if updateAccessSQL != nil {
			if err := conn.ExecTerminated(updateAccessSQL); err != nil {
				return log.Err(codes.ERR_UPDATE_ACCESS_TRIGGER, err).String("action", "update").Binary("sql", updateAccessSQL)
			}
		}

		if deleteAccessSQL != nil {
			if err := conn.ExecTerminated(deleteAccessSQL); err != nil {
				return log.Err(codes.ERR_UPDATE_ACCESS_TRIGGER, err).String("action", "delete").Binary("sql", deleteAccessSQL)
			}
		}

		if err := conn.Exec(`
			update sqlkite_tables
			set name = $1, definition = $2, updated = unixepoch()
			where name = $3
		`, tableName, definition, existingTableName); err != nil {
			return log.Err(codes.ERR_UPDATE_SQLKITE_TABLES, err)
		}

		return nil
	})

	if err != nil {
		return err
	}

	// update our in-memory project
	_, err = ReloadProject(p.Id)
	return err
}

func (p *Project) DeleteTable(env *Env, tableName string) error {
	if _, exists := p.tables[tableName]; !exists {
		env.VC.Invalid(UnknownTable(tableName))
		return nil
	}

	buffer := p.SQLBuffer()
	defer buffer.Release()

	buffer.Write([]byte("drop table "))
	buffer.WriteUnsafe(tableName)
	dropSQL, err := buffer.SqliteBytes()
	if err != nil {
		return p.bufferErrorToValidation(env, err, BUFFER_TYPE_GENERATE_SQL)
	}

	err = p.WithTransaction(func(conn sqlite.Conn) error {
		if err := conn.ExecTerminated(dropSQL); err != nil {
			return log.Err(codes.ERR_DELETE_TABLE_EXEC, err).Binary("sql", dropSQL)
		}

		if err := conn.Exec("delete from sqlkite_tables where name = $1", tableName); err != nil {
			return log.Err(codes.ERR_DELETE_SQLKITE_TABLES, err)
		}

		return nil
	})

	if err != nil {
		return err
	}

	// update our in-memory project
	_, err = ReloadProject(p.Id)
	return err
}

func (p *Project) Select(env *Env, sel sql.Select) (*QueryResult, error) {
	validator := env.VC

	// This is important. We're going to inject our access control into our select
	// query. We do this by examing every table in the select statement and seeing
	// if it has a configured access control CTE. If it does, we'll inform the
	// select query about the CTE and we'll mutate the table name to use the CTE
	// (sqlite CTEs don't shadow table names, so we need to give the CTE a distinct
	// name and change the query's table name(s) to use the CTE name(s)).
	selRef := &sel
	tables := p.tables
	for i, from := range sel.Froms {
		tableName := from.TableName()
		table := tables[tableName]
		if table == nil {
			// while we're looping through these tables, we might as well exit early
			// (and with a good errorr message) on an unknown table.
			validator.Invalid(UnknownTable(tableName))
		} else if selectAccess := table.Access.Select; selectAccess != nil {
			selRef.CTE(i, selectAccess.Name, selectAccess.CTE)
		}
	}

	if !validator.IsValid() {
		return nil, nil
	}

	return p.executeQueryWithResults(env, sel)
}

func (p *Project) Insert(env *Env, ins sql.Insert) (*QueryResult, error) {
	if len(ins.Returning) == 0 {
		return p.executeQueryWithoutResult(env, ins)
	} else {
		return p.executeQueryWithResults(env, ins)
	}
}

func (p *Project) Update(env *Env, upd sql.Update) (*QueryResult, error) {
	if len(upd.Returning) == 0 {
		return p.executeQueryWithoutResult(env, upd)
	} else {
		return p.executeQueryWithResults(env, upd)
	}
}

func (p *Project) Delete(env *Env, del sql.Delete) (*QueryResult, error) {
	if len(del.Returning) == 0 {
		return p.executeQueryWithoutResult(env, del)
	} else {
		return p.executeQueryWithResults(env, del)
	}
}

func (p *Project) SQLBuffer() *buffer.Buffer {
	// We get a buffer from our pool, but set the max to this project's max sql length
	// This allows us to share buffers across projects (which is going to be significantly
	// more memory efficient), while letting us specifying a per-project max sql length

	// caller must release this!
	return Buffer.CheckoutMax(p.Limits.MaxSQLLength)
}

func (p *Project) ResultBuffer() *buffer.Buffer {
	// see SQLBuffer
	return Buffer.CheckoutMax(p.Limits.MaxResultLength)
}

func (p *Project) buildAccessTrigger(access *TableAccessMutate) ([]byte, utils.Releasable, error) {
	if access == nil {
		return nil, utils.NoopReleasable, nil
	}

	buffer := p.SQLBuffer()
	access.WriteCreate(buffer)

	bytes, err := buffer.SqliteBytes()
	if err != nil {
		buffer.Release()
		return nil, utils.NoopReleasable, err
	}

	return bytes, buffer, nil
}

// The table is being updated and we need to figure out what to do about
// any access control trigger we have and my want to change or remove.
// Even if the new access control is nil, which means "keep whatever we have",
// if the table name has changed, we'll want to re-create the trigger using
// the new table name. This isn't strictly necessary, but it avoids a few potential
// issues down the road - like avoid conflicts if a new table with the old name
// is created and has its own triggers.
func (p *Project) buildAccessTriggerChange(change kdr.Value[*TableAccessMutate], tableName string, existingAccess *TableAccessMutate, existingTableName string) ([]byte, utils.Releasable, error) {
	access := change.Replacement
	if change.IsKeep() {
		if tableName == existingTableName || existingAccess == nil {
			// We're being told to keep what we have now AND (the table name hasn't
			// changed OR there was no access to begin with). In this case, there's
			// nothing for us to do.
			return nil, utils.NoopReleasable, nil
		}
		// We're told to "keep what we have", but our table name has changed. For the
		// purpose of generating our create trigger, we'll use the existing access
		// control (and just give it the new name later)
		access = existingAccess.Clone(tableName)
	}

	buffer := p.SQLBuffer()

	if existingAccess != nil {
		existingAccess.WriteDrop(buffer)
	}

	if access != nil {
		buffer.Write([]byte(";\n"))
		access.WriteCreate(buffer)
	}

	bytes, err := buffer.SqliteBytes()
	if err != nil {
		buffer.Release()
		return nil, utils.NoopReleasable, err
	}

	return bytes, buffer, nil
}

func (p *Project) bufferErrorToValidation(env *Env, err error, bufferType BufferType) error {
	if !errors.Is(err, buffer.ErrMaxSize) {
		// this should not be possible, and we want to know about it if it does happen
		log.Error("unknown buffer error").Err(err).Log()
		return err
	}

	var invalid *validation.Invalid
	if bufferType == BUFFER_TYPE_GENERATE_SQL {
		invalid = &validation.Invalid{
			Code:  codes.VAL_SQL_TOO_LONG,
			Error: "Generated SQL query exceeds maximum allowed length",
			Data:  validation.MaxData(int(p.Limits.MaxSQLLength)),
		}
	} else {
		invalid = &validation.Invalid{
			Code:  codes.VAL_RESULT_TOO_LONG,
			Error: "Result too large",
			Data:  validation.MaxData(int(p.Limits.MaxResultLength)),
		}
	}
	env.VC.Invalid(invalid)
	return nil
}

func (p *Project) executeQueryWithoutResult(env *Env, q sql.Query) (*QueryResult, error) {
	writer := p.SQLBuffer()
	defer writer.Release()

	q.Write(writer)
	sql, err := writer.Bytes()

	if err != nil {
		return nil, p.bufferErrorToValidation(env, err, BUFFER_TYPE_GENERATE_SQL)
	}

	affected := 0
	err = env.WithDB(func(conn sqlite.Conn) error {
		err := conn.ExecBArr(sql, q.Values())
		if err != nil {
			return err
		}
		affected = conn.Changes()
		return nil
	})
	return &QueryResult{RowCount: affected}, err
}

// TODO: This function represents the single best opportunity for performance
// improvement. The more closely we can tie the execution of the statement to the
// fasthttp response, the better.
// This function calls cgo multiple times (via the rows.Next and rows.Scan)
// and moves data from sqlite to the sqlite driver to our buffer to the http response.
// The result is _always_ 1 column (it's a json string) and multiple rows.
// We should be able to pass our buffer into C and let it populate it directly
// from SQLite. There will be challenges with respect to growing the buffer as
// needed, but I think the gymnatics will be well worth the effort.
func (p *Project) executeQueryWithResults(env *Env, q sql.Query) (*QueryResult, error) {
	rowCount := 0

	writer := p.SQLBuffer()
	defer writer.Release()

	q.Write(writer)
	sql, err := writer.Bytes()
	if err != nil {
		return nil, p.bufferErrorToValidation(env, err, BUFFER_TYPE_GENERATE_SQL)
	}

	// We don't release this here, unless there's an error, as
	// it needs to survive until the response is sent.
	result := p.ResultBuffer()

	err = env.WithDB(func(conn sqlite.Conn) error {
		rows := conn.RowsBArr(sql, q.Values())
		defer rows.Close()

		for rows.Next() {
			var b sqlite.RawBytes
			rows.Scan(&b)
			result.WritePad(b, 1)
			result.WriteByteUnsafe(',')
			rowCount += 1
		}

		if err := rows.Error(); err != nil {
			return err
		}

		if err := result.Error(); err != nil {
			return p.bufferErrorToValidation(env, err, BUFFER_TYPE_RESULT)
		}

		if rowCount > 0 {
			// trailing comma
			result.Truncate(1)
		}
		return nil
	})

	if err != nil {
		result.Release()
		return nil, err
	}

	return &QueryResult{
		Result:   result,
		RowCount: rowCount,
	}, nil
}

func loadProject(id string) (*Project, error) {
	projectData, err := super.DB.GetProject(id)
	if projectData == nil || err != nil {
		return nil, err
	}
	return NewProject(projectData, true)
}

// Called whenever a project is removed from the Projects map. There can be other
// references to p when this is called, including from other goroutines. The
// only guarantee we have is that when this function is called, subsequent calls
// to the Projects map will not return this reference.
func shutdownProject(p *Project) {
	go p.Shutdown()
}

func NewProject(projectData *data.Project, logProjectId bool) (*Project, error) {
	id := projectData.Id
	var logField log.Field
	if logProjectId {
		logField = log.NewField().String("pid", id).Finalize()
	}

	// we'll lazily set these when the first connection in our pool is created
	pageSize := 0
	maxPageCountSQL := ""

	limits := projectData.Limits

	dbPool, err := NewDBPool(limits.MaxConcurrency, id, func(conn sqlite.Conn) error {
		// On the first connection, we'll query sqlite for the configured page_size
		// and get our max_age_count pragma ready
		if pageSize == 0 {
			if err := conn.Row("pragma page_size").Scan(&pageSize); err != nil {
				return log.ErrData(codes.ERR_GET_PAGE_SIZE, err, map[string]any{"pid": id})
			}
			// could pageSize still be 0 here???
			maxPageCount := limits.MaxDatabaseSize / uint64(pageSize)
			maxPageCountSQL = fmt.Sprintf("pragma max_page_count=%d", maxPageCount)
		}

		conn.BusyTimeout(5 * time.Second)
		if err := conn.Exec(maxPageCountSQL); err != nil {
			return log.ErrData(codes.ERR_MAX_PAGE_COUNT, err, map[string]any{"pid": id, "sql": maxPageCountSQL})
		}

		// We only ever expect 1 row in here. We give it an id (PK), which will always
		// be 0, so that we can upsert. This will help protect against the chance
		// that we don't properly teardown this data between connection re-use
		if err := conn.Exec("create temp table sqlkite_user (id integer primary key not null, user_id text not null, role text not null)"); err != nil {
			return log.ErrData(codes.ERR_CREATE_SQLKITE_USER, err, map[string]any{"pid": id})
		}
		return nil
	})

	if err != nil {
		if dbPool != nil {
			dbPool.shutdown()
		}
		return nil, err
	}

	tables := make(map[string]*Table)

	conn := dbPool.Checkout()
	rows := conn.Rows("select name, definition from sqlkite_tables")
	for rows.Next() {
		var name string
		var definition []byte

		rows.Scan(&name, &definition)

		var table *Table
		err = json.Unmarshal(definition, &table)
		if err != nil {
			break
		}
		for i, c := range table.Columns {
			// This is awful, and possibly not needed. But if we don't do this, the
			// default for this column will be the base64 encoded string, rather than
			// the []byte. And it's unclear if that would ever be a problem, but it
			// would be easy to forget this little quirk in the future, and it would
			// certainly be unexpected.
			if c.Type == COLUMN_TYPE_BLOB && c.Default != nil {
				var encodeLength int
				stringDefault := utils.S2B(c.Default.(string))
				byteDefault := make([]byte, base64.StdEncoding.DecodedLen(len(stringDefault)))

				encodeLength, err = base64.StdEncoding.Decode(byteDefault, stringDefault)
				if err != nil {
					break
				}
				c.Default = byteDefault[:encodeLength]
				table.Columns[i] = c
			}
		}
		if s := table.Access.Select; s != nil {
			table.Access.Select.Name = "sqlkite_cte_" + name
		}
		tables[name] = table
	}
	rows.Close()
	dbPool.Release(conn)

	if err != nil {
		dbPool.shutdown()
		return nil, log.ErrData(codes.ERR_CREATE_READ_TABLE_DEFINITIONS, err, map[string]any{"pid": id})
	}
	// yes, rows.Error() is safe to call after Close
	if err := rows.Error(); err != nil {
		dbPool.shutdown()
		return nil, log.ErrData(codes.ERR_CREATE_READ_TABLE_ROWS, err, map[string]any{"pid": id})
	}

	return &Project{
		dbPool:   dbPool,
		tables:   tables,
		logField: logField,

		Id:     id,
		Limits: limits,
		Auth:   projectData.Auth,
		Debug:  projectData.Debug,

		// If we let this start at 0, then restarts are likely to produce duplicates.
		// While we make no guarantees about the uniqueness of the requestId, there's
		// no reason we can't help things out a little.
		requestId: uint32(time.Now().Unix()),
	}, nil
}

func ReloadProject(id string) (*Project, error) {
	project, err := loadProject(id)
	if err != nil {
		return nil, err
	}
	Projects.Put(id, project)
	return project, nil
}

func UnknownTable(tableName string) *validation.Invalid {
	return &validation.Invalid{
		Code:  codes.VAL_UNKNOWN_TABLE,
		Error: "Unknown table: " + tableName,
		Data:  validation.ValueData(tableName),
	}
}

// Take an existin Table and return a new Table which merges the changes
// from the TableAlter
func applyTableChanges(table *Table, existing *Table, alter TableAlter) {
	// tables / columns are immutable, we need to clone the columns to make sure
	// changes we make here aren't reflected in any existing references
	columns := make([]Column, len(existing.Columns))
	for i, c := range existing.Columns {
		columns[i] = c
	}

	renamed := false
	for _, change := range alter.Changes {
		switch c := change.(type) {
		case TableAlterRename:
			table.Name = c.To
			renamed = true
		case TableAlterAddColumn:
			columns = append(columns, c.Column)
		case TableAlterDropColumn:
			target := c.Name
			for i, column := range columns {
				if column.Name == target {
					for j := i + 1; j < len(columns); j++ {
						columns[j-1] = columns[j]
					}
					columns = columns[:len(columns)-1]
					break
				}
			}
		case TableAlterRenameColumn:
			target := c.Name
			for i, column := range columns {
				if column.Name == target {
					columns[i].Name = c.To
					break
				}
			}
		default:
			// should not be possible
			panic("Unknown TableAlterChange")
		}
	}
	table.Columns = columns

	if sa := alter.SelectAccess; sa.IsKeep() {
		// could be nil or not, either way, we're told to keep it as-is
		table.Access.Select = existing.Access.Select
	} else {
		// sa.Replacement is conveniently nil when Action == DELETE
		table.Access.Select = sa.Replacement
	}

	if ia := alter.InsertAccess; ia.IsKeep() {
		if existingAccess := existing.Access.Insert; existingAccess != nil && renamed {
			// the name of our table has changed, we need a new Access object based
			// on the new table name
			table.Access.Insert = existingAccess.Clone(table.Name)
		} else {
			// either nil or not renamed, either way, we can copy the existing access as-is
			table.Access.Insert = existingAccess
		}
	} else {
		// ia.Replacement is conveniently nil when Action == DELETE, so this handles
		// both the "delete" and the "replace" option
		table.Access.Insert = ia.Replacement
	}

	if ua := alter.UpdateAccess; ua.IsKeep() {
		if existingAccess := existing.Access.Update; existingAccess != nil && renamed {
			// the name of our table has changed, we need a new Access object based
			// on the new table name
			table.Access.Update = existingAccess.Clone(table.Name)
		} else {
			// ia.Replacement is conveniently nil when Action == DELETE, so this handles
			// both the "delete" and the "replace" option
			table.Access.Update = existingAccess
		}
	} else {
		// ua.Replacement is conveniently nil when Action == DELETE
		table.Access.Update = ua.Replacement
	}

	if da := alter.DeleteAccess; da.IsKeep() {
		if existingAccess := existing.Access.Delete; existingAccess != nil && renamed {
			// the name of our table has changed, we need a new Access object based
			// on the new table name
			table.Access.Delete = existingAccess.Clone(table.Name)
		} else {
			// either nil or not renamed, either way, we can copy the existing access as-is
			table.Access.Delete = existingAccess
		}
	} else {
		// ia.Replacement is conveniently nil when Action == DELETE, so this handles
		// both the "delete" and the "replace" option
		table.Access.Delete = da.Replacement
	}
}
