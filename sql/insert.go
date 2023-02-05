package sql

import (
	"src.goblgobl.com/utils/buffer"
)

type Insert struct {
	Into       string
	Columns    []string
	Parameters []any
}

func (i Insert) Write(b *buffer.Buffer) {
	columns := i.Columns

	b.Write([]byte("insert into "))
	b.WriteUnsafe(i.Into)

	b.Write([]byte(" ("))
	for _, c := range columns {
		b.WriteUnsafe(c)
		b.Write([]byte(", "))
	}
	b.Truncate(2) // strip out the last trailing comma

	b.Write([]byte(") values"))
	numberOfCols := len(columns)
	numberOfRows := len(i.Parameters) / numberOfCols

	// TODO: We can figure out how much space we'll need in our buffer to write
	// all our placeholders. We can use b.Pad(SIZE) to make sure we don't have
	// to keep allocating more space

	insertPlaceholders(0, numberOfCols, b)
	for i := 1; i < numberOfRows; i++ {
		insertPlaceholders(i, numberOfCols, b)
	}
}

func insertPlaceholders(rowIndex int, numberOfCols int, b *buffer.Buffer) {
	b.Write([]byte("\n ("))
	indexOffset := rowIndex * numberOfCols
	for i := 1; i <= numberOfCols; i++ {
		b.WritePad(placeholders[indexOffset+i], 1)
		b.WriteByte(',')
	}
	b.Truncate(1)
	b.WriteByte(')')
}
