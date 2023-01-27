package sql

import (
	"encoding/hex"
	"strconv"

	"src.goblgobl.com/sqlite"
	"src.goblgobl.com/sqlkite/data"
	"src.goblgobl.com/utils/buffer"
)

func CreateTable(table data.Table, buffer *buffer.Buffer) {
	columns := table.Columns

	buffer.Write([]byte("create table "))
	buffer.WriteUnsafe(table.Name)
	buffer.Write([]byte("(\n\t"))

	writeColumn(columns[0], buffer)
	for _, column := range columns[1:] {
		buffer.Write([]byte(",\n\t"))
		writeColumn(column, buffer)
	}
	buffer.Write([]byte("\n)"))
}

func writeColumn(column data.Column, buffer *buffer.Buffer) {
	tpe := column.Type

	buffer.WriteUnsafe(column.Name)
	buffer.WriteByte(' ')
	buffer.WriteUnsafe(tpe.String())
	if !column.Nullable {
		buffer.Write([]byte(" not"))
	}
	buffer.Write([]byte(" null"))
	if d := column.Default; d != nil {
		buffer.Write([]byte(" default("))
		switch tpe {
		case data.COLUMN_TYPE_INT:
			buffer.WriteUnsafe(strconv.Itoa(d.(int)))
		case data.COLUMN_TYPE_REAL:
			buffer.WriteUnsafe(strconv.FormatFloat(d.(float64), 'f', -1, 64))
		case data.COLUMN_TYPE_TEXT:
			buffer.WriteString(sqlite.EscapeLiteral(d.(string)))
		case data.COLUMN_TYPE_BLOB:
			buffer.Write([]byte("x'"))
			buffer.WriteString(hex.EncodeToString(d.([]byte)))
			buffer.WriteByte('\'')
		}
		buffer.WriteByte(')')
	}
}

type AlterTable struct {
	Name    string `json:"name"`
	Changes []Part `json:"changes"`
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
	Column data.Column `json:"column"`
}

func (a AddColumn) Write(b *buffer.Buffer) {
	b.Write([]byte("add column "))
	writeColumn(a.Column, b)
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
