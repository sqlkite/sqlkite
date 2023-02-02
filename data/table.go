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
