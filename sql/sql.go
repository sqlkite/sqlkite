package sql

import (
	"strconv"

	"src.goblgobl.com/utils/buffer"
)

type JoinType int
type DataFieldType int
type LogicalOperator int

const (
	MAX_PARAMETERS = 999

	JOIN_TYPE_NONE JoinType = iota
	JOIN_TYPE_INNER
	JOIN_TYPE_LEFT
	JOIN_TYPE_RIGHT
	JOIN_TYPE_FULL

	DATA_FIELD_PLACEHOLDER DataFieldType = iota
	DATA_FIELD_COLUMN

	LOGICAL_AND LogicalOperator = iota
	LOGICAL_OR
)

var placeholders = make([][]byte, MAX_PARAMETERS)

func init() {
	for i := 0; i < MAX_PARAMETERS; i++ {
		placeholders[i] = []byte("?" + strconv.Itoa(i))
	}
}

type Query interface {
	Part
	Values() []any
}

type Part interface {
	Write(*buffer.Buffer)
}

type Alias struct {
	Alias string `json:"alias",omitempty`
}

func (a *Alias) Write(b *buffer.Buffer) {
	b.Write([]byte(" as "))
	b.WriteUnsafe(a.Alias)
}

type TableName struct {
	Alias *Alias `json:"alias",omitempty`
	Name  string `json:"name"`
}

func (t TableName) Write(b *buffer.Buffer) {
	b.WriteUnsafe(t.Name)
	if alias := t.Alias; alias != nil {
		alias.Write(b)
	}
}

/*
Can represent either a column or an placeholder. For example, given:

	select id, full_name as name, ?1

"id", "full_name as name" and ?1 are each DataFields.

While placeholders are more rare in a select column list, the
column-or-placeholder duality is apparent in where conditions.
*/
type DataField struct {
	Alias *Alias        `json:"as",omitempty`    // can be empty
	Table string        `json:"table",omitempty` // either the full table name, or the table alias, or empty
	Name  string        `json:"name"`            // the column name or placeholder
	Type  DataFieldType `json:"type"`
}

func (df DataField) Write(b *buffer.Buffer) {
	if t := df.Table; t != "" {
		b.WriteUnsafe(t)
		b.WriteByte('.')
	}

	b.WriteUnsafe(df.Name)

	if alias := df.Alias; alias != nil {
		alias.Write(b)
	}
}

// instead of writing the typical data field as a selected column,
// e.g: t1.full_name as name
// we write it as an argument to the json_object function,
// e.g.: 'name', t1.full_name
func (df DataField) WriteAsJsonObject(b *buffer.Buffer) {
	name := df.Name

	b.WriteByte('\'')
	if alias := df.Alias; alias != nil {
		b.WriteUnsafe(alias.Alias)
	} else {
		b.WriteUnsafe(name)
	}
	b.Write([]byte("', "))
	if t := df.Table; t != "" {
		b.WriteUnsafe(t)
		b.WriteByte('.')
	}
	b.WriteUnsafe(name)
}

type CTE struct {
	Name string
	CTE  string
}

func (cte CTE) Write(b *buffer.Buffer) {
	b.WriteUnsafe(cte.Name)
	b.Write([]byte(" as ("))
	b.WriteUnsafe(cte.CTE)
	b.WriteByte(')')
}

type JoinableFrom struct {
	On    *Condition `json:"on",omitempty`
	Table TableName  `json:"table"`
	Join  JoinType   `json:"join",omitempty`
}

func (f JoinableFrom) TableName() string {
	return f.Table.Name
}

func (f JoinableFrom) Write(b *buffer.Buffer) {
	switch f.Join {
	case JOIN_TYPE_INNER:
		b.Write([]byte("inner join "))
	case JOIN_TYPE_LEFT:
		b.Write([]byte("left join "))
	case JOIN_TYPE_RIGHT:
		b.Write([]byte("right join "))
	case JOIN_TYPE_FULL:
		b.Write([]byte("full join "))
	}

	f.Table.Write(b)

	if condition := f.On; condition != nil {
		b.Write([]byte(" on "))
		condition.Write(b)
	}
}
