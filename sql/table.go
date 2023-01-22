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
