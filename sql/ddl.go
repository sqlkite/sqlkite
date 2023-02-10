package sql

import (
	"encoding/hex"
	"fmt"
	"strconv"

	"src.goblgobl.com/sqlite"
	"src.goblgobl.com/utils/buffer"
	"src.goblgobl.com/utils/optional"
)

type ColumnType int

const (
	COLUMN_TYPE_INVALID ColumnType = iota
	COLUMN_TYPE_INT
	COLUMN_TYPE_REAL
	COLUMN_TYPE_TEXT
	COLUMN_TYPE_BLOB
)

type Table struct {
	Name           string              `json:"name"`
	Columns        []Column            `json:"columns"`
	Access         TableAccess         `json:"access"`
	MaxDeleteCount optional.Value[int] `json:"max_delete_count"`
	MaxUpdateCount optional.Value[int] `json:"max_update_count"`
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
	b.Truncate(3)
	b.Write([]byte("\n)"))
}

type Column struct {
	Name     string     `json:"name"`
	Type     ColumnType `json:"type"`
	Default  any        `json:"default"`
	Nullable bool       `json:"nullable"`
}

func (c Column) Write(b *buffer.Buffer) {
	tpe := c.Type

	b.WriteUnsafe(c.Name)
	b.WriteByte(' ')
	b.WriteUnsafe(tpe.String())
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

func (c ColumnType) String() string {
	switch c {
	case COLUMN_TYPE_INT:
		return "int"
	case COLUMN_TYPE_REAL:
		return "real"
	case COLUMN_TYPE_TEXT:
		return "text"
	case COLUMN_TYPE_BLOB:
		return "blob"
	}
	panic(fmt.Sprintf("ColumnType.String(%d)", c))
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

type AlterTableChange interface {
	Part
}

type AlterTable struct {
	Name    string             `json:"name"`
	Changes []AlterTableChange `json:"changes"`
}

func (a *AlterTable) Write(b *buffer.Buffer) {
	name := a.Name
	for _, change := range a.Changes {
		b.Write([]byte("alter table "))
		b.WriteUnsafe(name)
		b.WriteByte(' ')
		change.Write(b)
		b.Write([]byte(";\n"))
	}
}

type AddColumn struct {
	Column Column `json:"column"`
}

func (a AddColumn) Write(b *buffer.Buffer) {
	b.Write([]byte("add column "))
	a.Column.Write(b)
}

type DropColumn struct {
	Name string `json:"name"`
}

func (d DropColumn) Write(b *buffer.Buffer) {
	b.Write([]byte("drop column "))
	b.WriteUnsafe(d.Name)
}

type RenameTable struct {
	To string `json:"to"`
}

func (r RenameTable) Write(b *buffer.Buffer) {
	b.Write([]byte("rename to "))
	b.WriteUnsafe(r.To)
}

type RenameColumn struct {
	Name string `json:"name"`
	To   string `json:"to"`
}

func (r RenameColumn) Write(b *buffer.Buffer) {
	b.Write([]byte("rename column "))
	b.WriteUnsafe(r.Name)
	b.Write([]byte(" to "))
	b.WriteUnsafe(r.To)
}

func TableAccessCreateTrigger(table string, action string, access *MutateTableAccess, buffer *buffer.Buffer) {
	buffer.Write([]byte("create trigger sqlkite_row_access_"))
	triggerNameSuffix(table, action, buffer)
	buffer.Write([]byte("\nbefore "))
	buffer.WriteUnsafe(action)

	buffer.Write([]byte(" on "))
	buffer.WriteUnsafe(table)
	buffer.Write([]byte(" for each row"))

	if w := access.When; w != "" {
		buffer.Write([]byte("\nwhen ("))
		buffer.WriteUnsafe(w)
		buffer.WriteByte(')')
	}

	buffer.Write([]byte("\nbegin\n "))
	trigger := access.Trigger
	buffer.WriteUnsafe(trigger)
	if trigger[len(trigger)-1] != ';' {
		buffer.WriteByte(';')
	}
	buffer.Write([]byte("\nend"))
}

func TableAccessDropTrigger(table string, action string, buffer *buffer.Buffer) {
	buffer.Write([]byte("drop trigger if exists sqlkite_row_access_"))
	triggerNameSuffix(table, action, buffer)
}

func triggerNameSuffix(table string, action string, buffer *buffer.Buffer) {
	buffer.WriteUnsafe(table)
	buffer.WriteByte('_')
	buffer.WriteUnsafe(action)
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