//go:build !release

// Used as a factory for tests only

package data

import (
	"strings"

	"src.goblgobl.com/utils/uuid"
)

type ColumnBuilder struct {
	column *Column
}

func BuildColumn() *ColumnBuilder {
	return &ColumnBuilder{
		column: &Column{
			Name:     uuid.String(),
			Nullable: true,
			Type:     COLUMN_TYPE_TEXT,
		},
	}
}

func (cb *ColumnBuilder) Name(name string) *ColumnBuilder {
	cb.column.Name = name
	return cb
}

func (cb *ColumnBuilder) Type(tpe string) *ColumnBuilder {
	switch strings.ToLower(tpe) {
	case "int":
		cb.column.Type = COLUMN_TYPE_INT
	case "real":
		cb.column.Type = COLUMN_TYPE_REAL
	case "text":
		cb.column.Type = COLUMN_TYPE_TEXT
	case "blob":
		cb.column.Type = COLUMN_TYPE_BLOB
	default:
		panic("unknown column type: " + tpe)
	}
	return cb
}

func (cb *ColumnBuilder) NotNullable() *ColumnBuilder {
	cb.column.Nullable = false
	return cb
}

func (cb *ColumnBuilder) Nullable() *ColumnBuilder {
	cb.column.Nullable = true
	return cb
}

func (cb *ColumnBuilder) Default(dflt any) *ColumnBuilder {
	cb.column.Default = dflt
	return cb
}

func (cb *ColumnBuilder) Column() Column {
	return *cb.column
}
