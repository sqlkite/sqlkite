package sql

// The sqlkite concept of a table has differences than the SQLite concept. For
// example, an sqlkite table can have a MaxUpdateCount or access control.
// Because of this extra data, we store the table definition in the sqlkite_tables
// table. But SQLite also knows a lot about the tables, such as columns, primary
// key, indexes, etc. Having this data maintained by both sqlkite and SQLite is
// problematic, because we need to keep it in sync.
// A lot of the SQLite data isn't something sqlkite really cares about. For example
// sqlkite doesn't care what columns are primary keys. There's no reason to store
// this data in the sqlkite_tables table...the best that can come from that is
// that we introduce an inconsistency. All of this data which sqlkite doesn't
// really care about (but which is still important, and which we at least need
// to be aware of when creating the table (to generate the correct SQL)), is stored
// in the TableExtended and ColumnExtended.

// SQLite has a noteworthy way to implement and declare auto-incrementing IDs.
// https://www.sqlite.org/autoinc.html  describes the behavior.
// With respect to what we're trying to do here (generate the create table statement),
// the main concern is that, to create a monotonic autoincrement primary key, we
// HAVE to use the  "column_name integer primary key autoincrement". Note that
// we HAVE to use "integer" and not "int" and note that the "primary key" statement
// has to be in the column definition, not as a separate statetement.

import (
	"encoding/hex"
	"fmt"
	"strconv"

	"src.goblgobl.com/sqlite"
	"src.goblgobl.com/utils/buffer"
	"src.goblgobl.com/utils/optional"
)

type ColumnType int
type AutoIncrementType int

const (
	COLUMN_TYPE_INVALID ColumnType = iota
	COLUMN_TYPE_INT
	COLUMN_TYPE_REAL
	COLUMN_TYPE_TEXT
	COLUMN_TYPE_BLOB
)
const (
	AUTO_INCREMENT_TYPE_NONE AutoIncrementType = iota
	AUTO_INCREMENT_TYPE_STRICT
	AUTO_INCREMENT_TYPE_REUSE
)

var (
	defaultTableExtended  = &TableExtended{}
	defaultColumnExtended = &ColumnExtended{}
)

type Table struct {
	Name           string              `json:"name"`
	Columns        []Column            `json:"columns"`
	Access         TableAccess         `json:"access"`
	MaxDeleteCount optional.Value[int] `json:"max_delete_count"`
	MaxUpdateCount optional.Value[int] `json:"max_update_count"`
	Extended       *TableExtended      `json:"-"`
}

func (t *Table) Column(name string) (Column, bool) {
	for _, c := range t.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return Column{}, false
}

func (t *Table) Write(b *buffer.Buffer) {
	extended := t.Extended
	if extended == nil {
		extended = defaultTableExtended
	}

	b.Write([]byte("create table "))
	b.WriteUnsafe(t.Name)
	b.Write([]byte("(\n\t"))

	columns := t.Columns
	for _, c := range columns {
		c.Write(b)
		b.Write([]byte(",\n\t"))
	}

	// If we have an auto-increment column, the we expect the "primary key" statement
	// to appear in the column definition (because this is what SQLite requires).
	if pk := extended.PrimaryKey; len(pk) > 0 {
		if !t.hasAutoIncrement(extended) {
			b.Write([]byte("primary key ("))
			for _, columnName := range pk {
				b.WriteUnsafe(columnName)
				b.WriteByte(',')
			}
			b.Truncate(1)
			b.Write([]byte("),\n\t"))
		}
	}

	b.Truncate(3)
	b.Write([]byte("\n)"))
}

func (t *Table) hasAutoIncrement(extended *TableExtended) bool {
	pk := extended.PrimaryKey

	// "autoincrement" is only valid when we have a 1 column primary key.
	if len(pk) != 1 {
		return false
	}

	columnName := pk[0]

	for _, column := range t.Columns {
		if column.Name != columnName {
			continue
		}

		if ex := column.Extended; ex != nil {
			return ex.AutoIncrement.Value != AUTO_INCREMENT_TYPE_NONE
		}
	}

	return false
}

type TableExtended struct {
	PrimaryKey []string
	Indexes    []Index
}

type Column struct {
	Name     string          `json:"name"`
	Type     ColumnType      `json:"type"`
	Default  any             `json:"default"`
	Nullable bool            `json:"nullable"`
	Extended *ColumnExtended `json:"-"`
}

func (c *Column) Write(b *buffer.Buffer) {
	extended := c.Extended
	if extended == nil {
		extended = defaultColumnExtended
	}

	b.WriteUnsafe(c.Name)
	b.WriteByte(' ')

	tpe := c.Type
	switch tpe {
	case COLUMN_TYPE_INT:
		if ai := extended.AutoIncrement; !ai.Exists {
			b.Write([]byte("int"))
		} else {
			switch ai.Value {
			case AUTO_INCREMENT_TYPE_REUSE:
				b.Write([]byte("integer primary key"))
			case AUTO_INCREMENT_TYPE_STRICT:
				b.Write([]byte("integer primary key autoincrement"))
			}
		}
	case COLUMN_TYPE_REAL:
		b.Write([]byte("real"))
	case COLUMN_TYPE_TEXT:
		b.Write([]byte("text"))
	case COLUMN_TYPE_BLOB:
		b.Write([]byte("blob"))
	default:
		panic(fmt.Sprintf("unknown column type: %d", tpe))
	}

	if !c.Nullable {
		b.Write([]byte(" not"))
	}

	b.Write([]byte(" null"))
	if d := c.Default; d != nil {
		b.Write([]byte(" default("))
		switch tpe {
		case COLUMN_TYPE_INT:
			b.WriteUnsafe(strconv.Itoa(d.(int)))
		case COLUMN_TYPE_REAL:
			b.WriteUnsafe(strconv.FormatFloat(d.(float64), 'f', -1, 64))
		case COLUMN_TYPE_TEXT:
			b.WriteString(sqlite.EscapeLiteral(d.(string)))
		case COLUMN_TYPE_BLOB:
			b.Write([]byte("x'"))
			b.WriteString(hex.EncodeToString(d.([]byte)))
			b.WriteByte('\'')
		}
		b.WriteByte(')')
	}
}

type ColumnExtended struct {
	AutoIncrement optional.Value[AutoIncrementType] `json:"autoincrement`
}

type Index struct {
	Name    string
	Columns []string
	Desc    []bool
	Unique  bool
}

type TableAccess struct {
	Select *SelectTableAccess `json:"select",omitempty`
	Insert *MutateTableAccess `json:"insert",omitempty`
	Update *MutateTableAccess `json:"update",omitempty`
	Delete *MutateTableAccess `json:"delete",omitempty`
}

// Table access for selects is implemented using a CTE
type SelectTableAccess struct {
	CTE string `json:"cte"`
	// Not persisted, but set when the table is loaded.
	// Makes it easier to deal with table renamed
	Name string `json:"-"`
}

// Table access for insert/update/delete is implemented using a trigger
// The following structure will translate into something like:
// create trigger sqlkite_row_access before (insert|update|delete) on $table
// for each row [when $when]
// begin ${triger} end
type MutateTableAccess struct {
	When    string `json:"when",omitempty`
	Trigger string `json:"trigger"`
}

func (m *MutateTableAccess) WriteCreate(b *buffer.Buffer, table string, action string) {
	b.Write([]byte("create trigger sqlkite_row_access_"))
	m.writeTriggerNameSuffix(b, table, action)
	b.Write([]byte("\nbefore "))
	b.WriteUnsafe(action)

	b.Write([]byte(" on "))
	b.WriteUnsafe(table)
	b.Write([]byte(" for each row"))

	if w := m.When; w != "" {
		b.Write([]byte("\nwhen ("))
		b.WriteUnsafe(w)
		b.WriteByte(')')
	}

	b.Write([]byte("\nbegin\n "))
	trigger := m.Trigger
	b.WriteUnsafe(trigger)
	if trigger[len(trigger)-1] != ';' {
		b.WriteByte(';')
	}
	b.Write([]byte("\nend"))
}

func (m *MutateTableAccess) WriteDrop(b *buffer.Buffer, table string, action string) {
	b.Write([]byte("drop trigger if exists sqlkite_row_access_"))
	m.writeTriggerNameSuffix(b, table, action)
}

func (_ *MutateTableAccess) writeTriggerNameSuffix(b *buffer.Buffer, table string, action string) {
	b.WriteUnsafe(table)
	b.WriteByte('_')
	b.WriteUnsafe(action)
}

// yes, we need to sub-select in order to ensure json_group_array aggregates
// in the correct order

// select i.name, i."unique", i.partial, i.origin, m.sql, (
//   select json_group_array(json_object('c', name, 'd', desc))
//   from (
//     select name, desc
//     from pragma_index_xinfo(i.name)
//     where key = 1
//     order by seqno
//   )
// )
// from pragma_index_list('x') as i
//   left join sqlite_master m on i.name = m.name
// where (m.tbl_name = 'x' or m.tbl_name is null)

// index [unique] (columns [desc|asc])+ [where ...]
// primary key [X]
// primary key [X]

// int primary key
// autoincrement monotonically  <- include "autoincrement"
// autoincrement re-use <- don't include "autoincrement"
// either way: autoincrement cannot be used on a composite key

// Index
// Name text
// Unique bool
// Condition ...
// Columns [Name + order]
