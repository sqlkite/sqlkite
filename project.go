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
	Projects            concurrent.Map[*Project]
	CLEAR_SQLKITE_USERS = []byte(sqlite.Terminate("update sqlkite_user set id = '', role = ''"))
)

func init() {
	Projects = concurrent.NewMap[*Project](loadProject, shutdownProject)
}

// A project instance isn't updated. If the project is changed,
// a new instance is created.
type Project struct {
	// Project-specific counter for generating the RequestId
	requestId uint32

	// Any log entry generate for this project should include
	// the pid=$id field
	logField log.Field

	// The pool of SQLite connections to this database. The size of this
	// pool is controlled by the MaxConcurrency value.
	dbPool *DBPool

	Id string

	// When enabled, error response bodies may include additional data. This data
	// could leak internal implementation details (e.g. table schemas). But having
	// more data available in the response, without having to check a console or
	// central log system, is pretty valuable.
	Debug bool

	MaxConcurrency       uint16
	MaxSQLLength         uint32
	MaxSQLParameterCount uint16
	MaxDatabaseSize      uint64
	MaxRowCount          uint16
	MaxResultLength      uint32
	MaxFromCount         uint16
	MaxSelectColumnCount uint16
	MaxConditionCount    uint16
	MaxOrderByCount      uint16
	MaxTableCount        uint16
	tables               map[string]data.Table
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

func (p *Project) Table(name string) (data.Table, bool) {
	table, ok := p.tables[name]
	return table, ok
}

func (p *Project) WithDB(cb func(sqlite.Conn)) {
	pool := p.dbPool
	conn := pool.Checkout()
	defer pool.Release(conn)
	cb(conn)
}

func (p *Project) WithDBEnv(env *Env, cb func(sqlite.Conn) error) error {
	pool := p.dbPool
	conn := pool.Checkout()

	if user := env.User; user != nil {
		if err := conn.Exec("insert or replace into sqlkite_user (id, user_id, role) values (0, ?1, ?2)", user.Id, user.Role); err != nil {
			env.Error("WithDBEnv.upsert").String("uid", user.Id).String("role", user.Role).Err(err).Log()
			return err
		}
	}

	defer func() {
		if err := conn.ExecTerminated(CLEAR_SQLKITE_USERS); err != nil {
			env.Error("WithDBEnv.clear").Err(err).Log()
		}
		pool.Release(conn)
	}()

	return cb(conn)
}

func (p *Project) WithTransaction(cb func(sqlite.Conn) error) (err error) {
	p.WithDB(func(conn sqlite.Conn) {
		err = conn.Transaction(func() error {
			return cb(conn)
		})
	})
	return
}

func (p *Project) CreateTable(env *Env, table data.Table) error {
	if max := p.MaxTableCount; len(p.tables) >= int(max) {
		env.Validator.Add(validation.Invalid{
			Code:  codes.VAL_TOO_MANY_TABLES,
			Error: "Maximum table count reached",
			Data:  validation.Max(max),
		})
		return nil
	}

	definition, err := json.Marshal(table)
	if err != nil {
		return log.Err(codes.ERR_TABLE_SERIALIZE, err)
	}

	createBuffer := p.SQLBuffer()
	defer createBuffer.Release()

	sql.CreateTable(table, createBuffer)
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
	insertAccessSQL, insertAccessBuffer, err := p.buildAccessTrigger(tableName, "insert", access.Insert)
	if err != nil {
		return p.bufferErrorToValidation(env, err, BUFFER_TYPE_GENERATE_SQL)
	}
	defer insertAccessBuffer.Release()

	updateAccessSQL, udpdateAccessBuffer, err := p.buildAccessTrigger(tableName, "update", access.Update)
	if err != nil {
		return p.bufferErrorToValidation(env, err, BUFFER_TYPE_GENERATE_SQL)
	}
	defer udpdateAccessBuffer.Release()

	deleteAccessSQL, deleteAccessBuffer, err := p.buildAccessTrigger(tableName, "delete", access.Delete)
	if err != nil {
		return p.bufferErrorToValidation(env, err, BUFFER_TYPE_GENERATE_SQL)
	}
	defer deleteAccessBuffer.Release()

	err = p.WithTransaction(func(conn sqlite.Conn) error {
		if err := conn.ExecTerminated(createSQL); err != nil {
			return log.Err(codes.ERR_CREATE_TABLE_EXEC, err)
		}

		if insertAccessSQL != nil {
			if err := conn.ExecTerminated(insertAccessSQL); err != nil {
				return log.Err(codes.ERR_CREATE_ACCESS_TRIGGER, err).String("action", "insert")
			}
		}

		if updateAccessSQL != nil {
			if err := conn.ExecTerminated(updateAccessSQL); err != nil {
				return log.Err(codes.ERR_CREATE_ACCESS_TRIGGER, err).String("action", "update")
			}
		}

		if deleteAccessSQL != nil {
			if err := conn.ExecTerminated(deleteAccessSQL); err != nil {
				return log.Err(codes.ERR_CREATE_ACCESS_TRIGGER, err).String("action", "delete")
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

func (p *Project) UpdateTable(env *Env, alter sql.AlterTable, access data.TableAccess) error {
	existing, exists := p.tables[alter.Name]
	if !exists {
		env.Validator.Add(unknownTable(alter.Name))
		return nil
	}

	table := applyTableChanges(existing, alter, access)

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

	// See comments in CreateTable about my conflicting feelings about how this
	// should be done.
	tableName := table.Name
	existingAccess := existing.Access
	existingTableName := existing.Name

	insertAccessSQL, insertAccessBuffer, err := p.buildAccessTriggerChange(tableName, "insert", access.Insert, existingTableName, existingAccess.Insert)
	if err != nil {
		return p.bufferErrorToValidation(env, err, BUFFER_TYPE_GENERATE_SQL)
	}
	defer insertAccessBuffer.Release()

	updateAccessSQL, udpdateAccessBuffer, err := p.buildAccessTriggerChange(tableName, "update", access.Update, existingTableName, existingAccess.Update)
	if err != nil {
		return p.bufferErrorToValidation(env, err, BUFFER_TYPE_GENERATE_SQL)
	}
	defer udpdateAccessBuffer.Release()

	deleteAccessSQL, deleteAccessBuffer, err := p.buildAccessTriggerChange(tableName, "delete", access.Delete, existingTableName, existingAccess.Delete)
	if err != nil {
		return p.bufferErrorToValidation(env, err, BUFFER_TYPE_GENERATE_SQL)
	}
	defer deleteAccessBuffer.Release()

	err = p.WithTransaction(func(conn sqlite.Conn) error {
		if err := conn.ExecTerminated(alterSQL); err != nil {
			return log.Err(codes.ERR_UPDATE_TABLE_EXEC, err)
		}

		if insertAccessSQL != nil {
			if err := conn.ExecTerminated(insertAccessSQL); err != nil {
				return log.Err(codes.ERR_UPDATE_ACCESS_TRIGGER, err).String("action", "insert")
			}
		}

		if updateAccessSQL != nil {
			if err := conn.ExecTerminated(updateAccessSQL); err != nil {
				return log.Err(codes.ERR_UPDATE_ACCESS_TRIGGER, err).String("action", "update")
			}
		}

		if deleteAccessSQL != nil {
			if err := conn.ExecTerminated(deleteAccessSQL); err != nil {
				return log.Err(codes.ERR_UPDATE_ACCESS_TRIGGER, err).String("action", "delete")
			}
		}

		if err := conn.Exec("update sqlkite_tables set name = $1, definition = $2 where name = $3", tableName, definition, existing.Name); err != nil {
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
		env.Validator.Add(unknownTable(tableName))
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
			return log.Err(codes.ERR_DELETE_TABLE_EXEC, err)
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
	validator := env.Validator

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
		table, exists := tables[tableName]
		if !exists {
			// while we're looping through these tables, we might as well exit early
			// (and with a good errorr message) on an unknown table.
			validator.Add(unknownTable(tableName))
		}
		if selectAccess := table.Access.Select; selectAccess != nil {
			selRef.CTE(i, selectAccess.Name, selectAccess.CTE)
		}
	}

	if !validator.IsValid() {
		return nil, nil
	}

	return p.executeQueryWithResults(env, sel)
}

func (p *Project) Insert(env *Env, ins sql.Insert) (*QueryResult, error) {
	validator := env.Validator

	tableName := ins.Into.Name
	_, exists := p.tables[tableName]
	if !exists {
		validator.Add(unknownTable(tableName))
		return nil, nil
	}

	if len(ins.Returning) == 0 {
		return p.executeQueryWithoutResult(env, ins)
	} else {
		return p.executeQueryWithResults(env, ins)
	}
}

func (p *Project) SQLBuffer() *buffer.Buffer {
	// We get a buffer from our pool, but set the max to this project's max sql length
	// This allows us to share buffers across projects (which is going to be significantly
	// more memory efficient), while letting us specifying a per-project max sql length

	// caller must release this!
	return Buffer.CheckoutMax(p.MaxSQLLength)
}

func (p *Project) ResultBuffer() *buffer.Buffer {
	// see SQLBuffer
	return Buffer.CheckoutMax(p.MaxResultLength)
}

func (p *Project) buildAccessTrigger(tableName string, action string, access *data.MutateTableAccess) ([]byte, utils.Releasable, error) {
	if access == nil {
		return nil, utils.NoopReleasable, nil
	}

	buffer := p.SQLBuffer()
	sql.TableAccessCreateTrigger(tableName, action, access, buffer)

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
func (p *Project) buildAccessTriggerChange(tableName string, action string, access *data.MutateTableAccess, existingTableName string, existingAccess *data.MutateTableAccess) ([]byte, utils.Releasable, error) {
	if access == nil {
		if tableName == existingTableName || existingAccess == nil {
			// the new access is nil (which means "keep what we have")
			// and our table name hasn't changed or there was also no existing
			// access, there's nothing to do
			return nil, utils.NoopReleasable, nil
		}
		// We're told to "keep what we have", but our table name has changed. For the
		// purpose of generating our create trigger, we'll use the existing access
		// control (and just give it the new name later)
		access = existingAccess
	}

	buffer := p.SQLBuffer()

	// there might not be an existing trigger, but we drop it with an "if exists"
	// so it's simply to just always try to drop it
	sql.TableAccessDropTrigger(existingTableName, action, buffer)
	if access.Trigger != "" {
		// if the trigger is empty, it means remove (which we always do)
		// if it isn't empty, it means replace, which involved drop + create
		buffer.Write([]byte(";\n"))
		sql.TableAccessCreateTrigger(tableName, action, access, buffer)
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

	var invalid validation.Invalid
	if bufferType == BUFFER_TYPE_GENERATE_SQL {
		invalid = validation.Invalid{
			Code:  codes.VAL_SQL_TOO_LONG,
			Error: "Generated SQL query exceeds maximum allowed length",
			Data:  validation.Max(p.MaxSQLLength),
		}
	} else {
		invalid = validation.Invalid{
			Code:  codes.VAL_RESULT_TOO_LONG,
			Error: "Result too large",
			Data:  validation.Max(p.MaxResultLength),
		}
	}
	env.Validator.Add(invalid)
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
	err = p.WithDBEnv(env, func(conn sqlite.Conn) error {
		err := conn.ExecBArr(sql, q.Values())
		if err != nil {
			return err
		}
		affected = conn.Changes()
		return nil
	})
	return &QueryResult{RowCount: affected}, err
}

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

	err = p.WithDBEnv(env, func(conn sqlite.Conn) error {
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

	maxConcurrency := projectData.MaxConcurrency
	maxDatabaseSize := projectData.MaxDatabaseSize

	dbPool, err := NewDBPool(maxConcurrency, id, func(conn sqlite.Conn) error {
		// On the first connection, we'll query sqlite for the configured page_size
		// and get our max_age_count pragma ready
		if pageSize == 0 {
			if err := conn.Row("pragma page_size").Scan(&pageSize); err != nil {
				return log.ErrData(codes.ERR_GET_PAGE_SIZE, err, map[string]any{"pid": id})
			}
			// could pageSize still be 0 here???
			maxPageCount := maxDatabaseSize / uint64(pageSize)
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

	tables := make(map[string]data.Table)

	conn := dbPool.Checkout()
	rows := conn.Rows("select name, definition from sqlkite_tables")
	for rows.Next() {
		var name string
		var definition []byte

		rows.Scan(&name, &definition)

		var table data.Table
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
			if c.Type == data.COLUMN_TYPE_BLOB && c.Default != nil {
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

		Id:    id,
		Debug: projectData.Debug,

		MaxConcurrency:       maxConcurrency,
		MaxDatabaseSize:      maxDatabaseSize,
		MaxSQLLength:         projectData.MaxSQLLength,
		MaxSQLParameterCount: projectData.MaxSQLParameterCount,
		MaxRowCount:          projectData.MaxRowCount,
		MaxResultLength:      projectData.MaxResultLength,
		MaxFromCount:         projectData.MaxFromCount,
		MaxSelectColumnCount: projectData.MaxSelectColumnCount,
		MaxConditionCount:    projectData.MaxConditionCount,
		MaxOrderByCount:      projectData.MaxOrderByCount,
		MaxTableCount:        projectData.MaxTableCount,

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

func unknownTable(tableName string) validation.Invalid {
	return validation.Invalid{
		Code:  codes.VAL_UNKNOWN_TABLE,
		Error: "Unknown table: " + tableName,
		Data:  validation.Value(tableName),
	}
}

// Take an existin data.Table and return a new data.Table which merges the changes
// from the sql.AlterTable applied and the new data.TableAccess
func applyTableChanges(table data.Table, alter sql.AlterTable, access data.TableAccess) data.Table {
	for _, change := range alter.Changes {
		switch c := change.(type) {
		case sql.RenameTable:
			table.Name = c.To
		case sql.AddColumn:
			table.Columns = append(table.Columns, c.Column)
		case sql.DropColumn:
			target := c.Name
			columns := table.Columns
			for i, column := range columns {
				if column.Name == target {
					for j := i + 1; j < len(columns); j++ {
						columns[j-1] = columns[j]
					}
					table.Columns = columns[:len(columns)-1]
					break
				}
			}
		case sql.RenameColumn:
			target := c.Name
			for i, column := range table.Columns {
				if column.Name == target {
					table.Columns[i].Name = c.To
					break
				}
			}
		default:
			// should not be possible
			panic("Unknown AlterTableChange")
		}
	}

	// empty means erase
	// nil means keep the existing one
	// kind of feels like the opposite of what you'd expect
	if s := access.Select; s != nil {
		if s.CTE == "" {
			table.Access.Select = nil
		} else {
			table.Access.Select = s
		}
	}

	if access := access.Insert; access != nil {
		if access.Trigger == "" {
			table.Access.Insert = nil
		} else {
			table.Access.Insert = access
		}
	}

	if access := access.Update; access != nil {
		if access.Trigger == "" {
			table.Access.Update = nil
		} else {
			table.Access.Update = access
		}
	}

	if access := access.Delete; access != nil {
		if access.Trigger == "" {
			table.Access.Delete = nil
		} else {
			table.Access.Delete = access
		}
	}

	return table
}
