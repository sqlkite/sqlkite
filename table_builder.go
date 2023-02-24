//go:build !release

// Used as a factory for tests only

package sqlkite

import (
	"strings"

	"src.goblgobl.com/utils/optional"
	"src.goblgobl.com/utils/uuid"
	"src.goblgobl.com/utils/validation"
)

type ColumnBuilder struct {
	column *Column
}

func BuildColumn() *ColumnBuilder {
	return &ColumnBuilder{
		column: &Column{
			Name:       uuid.String(),
			Nullable:   true,
			Unique:     false,
			DenyInsert: false,
			DenyUpdate: false,
			Type:       COLUMN_TYPE_TEXT,
		},
	}
}

func (cb *ColumnBuilder) Name(name string) *ColumnBuilder {
	cb.column.Name = name
	cb.column.Field = &validation.Field{Flat: name}
	return cb
}

func (cb *ColumnBuilder) Type(tpe string) *ColumnBuilder {
	switch strings.ToLower(tpe) {
	case "int":
		cb.column.Type = COLUMN_TYPE_INT
		cb.column.Extension = new(ColumnIntExtension)
	case "real":
		cb.column.Type = COLUMN_TYPE_REAL
		cb.column.Extension = new(ColumnFloatExtension)
	case "text":
		cb.column.Type = COLUMN_TYPE_TEXT
		cb.column.Extension = new(ColumnTextExtension)
	case "blob":
		cb.column.Type = COLUMN_TYPE_BLOB
		cb.column.Extension = new(ColumnBlobExtension)
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

func (cb *ColumnBuilder) Unique() *ColumnBuilder {
	cb.column.Unique = true
	return cb
}

func (cb *ColumnBuilder) DenyInsert() *ColumnBuilder {
	cb.column.DenyInsert = true
	return cb
}

func (cb *ColumnBuilder) DenyUpdate() *ColumnBuilder {
	cb.column.DenyUpdate = true
	return cb
}

func (cb *ColumnBuilder) Default(dflt any) *ColumnBuilder {
	cb.column.Default = dflt
	return cb
}

func (cb *ColumnBuilder) Min(min any) *ColumnBuilder {
	switch cb.column.Type {
	case COLUMN_TYPE_INT:
		cb.column.Extension.(*ColumnIntExtension).Min = optional.New(min.(int))
	case COLUMN_TYPE_REAL:
		cb.column.Extension.(*ColumnFloatExtension).Min = optional.New(min.(float64))
	case COLUMN_TYPE_TEXT:
		cb.column.Extension.(*ColumnTextExtension).Min = optional.New(min.(int))
	case COLUMN_TYPE_BLOB:
		cb.column.Extension.(*ColumnBlobExtension).Min = optional.New(min.(int))
	}
	return cb
}

func (cb *ColumnBuilder) Max(max any) *ColumnBuilder {
	switch cb.column.Type {
	case COLUMN_TYPE_INT:
		cb.column.Extension.(*ColumnIntExtension).Max = optional.New(max.(int))
	case COLUMN_TYPE_REAL:
		cb.column.Extension.(*ColumnFloatExtension).Max = optional.New(max.(float64))
	case COLUMN_TYPE_TEXT:
		cb.column.Extension.(*ColumnTextExtension).Max = optional.New(max.(int))
	case COLUMN_TYPE_BLOB:
		cb.column.Extension.(*ColumnBlobExtension).Max = optional.New(max.(int))
	}
	return cb
}

func (cb *ColumnBuilder) Pattern(pattern string) *ColumnBuilder {
	switch cb.column.Type {
	case COLUMN_TYPE_TEXT:
		cb.column.Extension.(*ColumnTextExtension).Pattern = pattern
	default:
		panic("Pattern is only valid for Text column")
	}
	return cb
}

func (cb *ColumnBuilder) Choices(choices ...string) *ColumnBuilder {
	switch cb.column.Type {
	case COLUMN_TYPE_TEXT:
		cb.column.Extension.(*ColumnTextExtension).Choices = choices
	default:
		panic("Choices is only valid for Text column")
	}
	return cb
}

func (cb *ColumnBuilder) Column() *Column {
	return cb.column
}
