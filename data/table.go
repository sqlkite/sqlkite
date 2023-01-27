package data

import "fmt"

type ColumnType int

const (
	COLUMN_TYPE_INVALID ColumnType = iota
	COLUMN_TYPE_INT
	COLUMN_TYPE_REAL
	COLUMN_TYPE_TEXT
	COLUMN_TYPE_BLOB
)

type Table struct {
	Name    string      `json:"name"`
	Columns []Column    `json:"columns"`
	Access  TableAccess `json:"access"`
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

type TableAccess struct {
	Select *SelectTableAccess `json:"select",omitempty`
}

type SelectTableAccess struct {
	CTE string `json:"cte"`
	// Not persisted, but set when the table is loaded.
	// Makes it easier to deal with table renamed
	Name string `json:"-"`
}
