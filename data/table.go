package data

import "fmt"

type ColumnType int

const (
	COLUMN_TYPE_INT ColumnType = iota
	COLUMN_TYPE_REAL
	COLUMN_TYPE_TEXT
	COLUMN_TYPE_BLOB
)

type Table struct {
	Name    string `json:"name"`
	Columns []Column
	Select  *SelectAccessControl `json:"select",omitempty`
}

type Column struct {
	Name     string     `json:"name"`
	Type     ColumnType `json:"type"`
	Default  any        `json:"default"`
	Nullable bool       `json:"nullable"`
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

type SelectAccessControl struct {
	CTE  string `json:"cte"`
	Name string `json:"name"`
}
