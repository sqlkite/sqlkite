package sqlkite

// The sqlkite concept of a table has differences than the SQLite concept. For
// example, an sqlkite table can have a MaxUpdateCount or access control.
// Because of this extra data, we store the table definition in the sqlkite_tables
// table. But SQLite also knows a lot about the tables, such as columns, primary
// key, indexes, etc. There's overlap between what we keep in our "table" and
// what SQLite inherently knows. This duplication isn't ideal, and I am
// worried about things falling out of sync (what if someone renames a column
// directly in the database?). One thing that makes this a lot easier is that
// SQLite seriously limits the types of changes that can be made to a table (e.g.
// the primary key can't be changed after the fact), so, for now, we just deal
// with it.

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
	"src.goblgobl.com/utils/kdr"
	"src.goblgobl.com/utils/optional"
	"src.sqlkite.com/sqlkite/sql"
)

type ColumnType int
type AutoIncrementType int
type TableAccessMutateType int

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

const (
	TABLE_ACCESS_MUTATE_INSERT TableAccessMutateType = iota
	TABLE_ACCESS_MUTATE_UPDATE
	TABLE_ACCESS_MUTATE_DELETE
)

var (
	defaultColumnExtended = &ColumnExtended{}
)

type Table struct {
	Name           string       `json:"name"`
	Columns        []Column     `json:"columns"`
	Access         TableAccess  `json:"access"`
	MaxDeleteCount optional.Int `json:"max_delete_count"`
	MaxUpdateCount optional.Int `json:"max_update_count"`
	PrimaryKey     []string     `json:"primary_key"`
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
	if pk := t.PrimaryKey; len(pk) > 0 {
		if !t.hasAutoIncrement(pk) {
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

func (t *Table) hasAutoIncrement(pk []string) bool {
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

type TableAccess struct {
	Select *TableAccessSelect `json:"select",omitempty`
	Insert *TableAccessMutate `json:"insert",omitempty`
	Update *TableAccessMutate `json:"update",omitempty`
	Delete *TableAccessMutate `json:"delete",omitempty`
}

// Table access for selects is implemented using a CTE
type TableAccessSelect struct {
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
type TableAccessMutate struct {
	Name    string                `json:"name"`
	Type    TableAccessMutateType `json:"type"`
	When    string                `json:"when",omitempty`
	Trigger string                `json:"trigger"`
}

func NewTableAccessMutate(table string, tpe TableAccessMutateType, trigger string, when string) *TableAccessMutate {
	var name string
	switch tpe {
	case TABLE_ACCESS_MUTATE_INSERT:
		name = "sqlkite_ra_" + table + "_i"
	case TABLE_ACCESS_MUTATE_UPDATE:
		name = "sqlkite_ra_" + table + "_u"
	case TABLE_ACCESS_MUTATE_DELETE:
		name = "sqlkite_ra_" + table + "_d"
	}

	return &TableAccessMutate{
		Name:    name,
		Type:    tpe,
		When:    when,
		Trigger: trigger,
	}
}

func (m *TableAccessMutate) WriteCreate(b *buffer.Buffer) {
	name := m.Name

	b.Write([]byte("create trigger "))
	b.WriteUnsafe(name)
	b.Write([]byte("\nbefore "))

	switch m.Type {
	case TABLE_ACCESS_MUTATE_INSERT:
		b.Write([]byte("insert"))
	case TABLE_ACCESS_MUTATE_UPDATE:
		b.Write([]byte("update"))
	case TABLE_ACCESS_MUTATE_DELETE:
		b.Write([]byte("delete"))
	}

	b.Write([]byte(" on "))
	b.WriteUnsafe(name[11 : len(name)-2]) // awful
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

func (m *TableAccessMutate) WriteDrop(b *buffer.Buffer) {
	b.Write([]byte("drop trigger if exists "))
	b.WriteUnsafe(m.Name)
}

func (m *TableAccessMutate) Clone(tableName string) *TableAccessMutate {
	return NewTableAccessMutate(tableName, m.Type, m.Trigger, m.When)
}

type TableAlter struct {
	Name         string     `json:"name"`
	Changes      []sql.Part `json:"changes"`
	SelectAccess kdr.Value[*TableAccessSelect]
	InsertAccess kdr.Value[*TableAccessMutate]
	UpdateAccess kdr.Value[*TableAccessMutate]
	DeleteAccess kdr.Value[*TableAccessMutate]
}

func (a *TableAlter) Write(b *buffer.Buffer) {
	name := a.Name
	for _, change := range a.Changes {
		b.Write([]byte("alter table "))
		b.WriteUnsafe(name)
		b.WriteByte(' ')
		change.Write(b)
		b.Write([]byte(";\n"))
	}
}

type TableAlterAddColumn struct {
	Column Column `json:"column"`
}

func (a TableAlterAddColumn) Write(b *buffer.Buffer) {
	b.Write([]byte("add column "))
	a.Column.Write(b)
}

type TableAlterDropColumn struct {
	Name string `json:"name"`
}

func (d TableAlterDropColumn) Write(b *buffer.Buffer) {
	b.Write([]byte("drop column "))
	b.WriteUnsafe(d.Name)
}

type TableAlterRename struct {
	To string `json:"to"`
}

func (r TableAlterRename) Write(b *buffer.Buffer) {
	b.Write([]byte("rename to "))
	b.WriteUnsafe(r.To)
}

type TableAlterRenameColumn struct {
	Name string `json:"name"`
	To   string `json:"to"`
}

func (r TableAlterRenameColumn) Write(b *buffer.Buffer) {
	b.Write([]byte("rename column "))
	b.WriteUnsafe(r.Name)
	b.Write([]byte(" to "))
	b.WriteUnsafe(r.To)
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
