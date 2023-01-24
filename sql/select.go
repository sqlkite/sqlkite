package sql

import (
	"strconv"

	"src.goblgobl.com/utils/buffer"
	"src.goblgobl.com/utils/optional"
)

type JoinType int

const (
	JOIN_TYPE_NONE JoinType = iota
	JOIN_TYPE_INNER
	JOIN_TYPE_LEFT
	JOIN_TYPE_RIGHT
	JOIN_TYPE_FULL
)

type Select struct {
	Columns    []DataField
	Froms      []SelectFrom
	Where      Condition
	Parameters []any
	Limit      int
	Offset     optional.Value[int]
	OrderBy    []OrderBy
}

func (s Select) Write(b *buffer.Buffer) {
	b.Write([]byte("select json_object("))
	columns := s.Columns
	columns[0].WriteAsJsonObject(b)
	for _, column := range columns[1:] {
		b.Write([]byte(", "))
		column.WriteAsJsonObject(b)
	}

	b.Write([]byte(") from "))
	froms := s.Froms
	froms[0].Write(b)
	for _, from := range froms[1:] {
		b.WriteByte(' ')
		from.Write(b)
	}

	b.Write([]byte(" where "))
	s.Where.Write(b)

	orderBy := s.OrderBy
	if len(orderBy) > 0 {
		b.Write([]byte(" order by "))
		orderBy[0].Write(b)
		for _, orderBy := range orderBy[1:] {
			b.WriteByte(',')
			orderBy.Write(b)
		}
	}

	b.Write([]byte(" limit "))
	b.WriteString(strconv.Itoa(s.Limit))
	if offset := s.Offset; offset.Exists {
		b.Write([]byte(" offset "))
		b.WriteString(strconv.Itoa(offset.Value))
	}
}

type SelectFrom struct {
	Join JoinType   `json:"join",omitempty`
	From From       `json:"table"`
	On   *Condition `json:"on",omitempty`
}

func (f SelectFrom) TableName() string {
	return f.From.Table
}

func (f SelectFrom) Write(b *buffer.Buffer) {
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

	f.From.Write(b)

	if condition := f.On; condition != nil {
		b.Write([]byte(" on "))
		condition.Write(b)
	}
}

type OrderBy struct {
	Field DataField
	Desc  bool
}

func (o OrderBy) Write(b *buffer.Buffer) {
	df := o.Field
	if t := df.Table; t != "" {
		b.WriteUnsafe(t)
		b.WriteByte('.')
	}
	b.WriteUnsafe(df.Name)
	if o.Desc {
		b.Write([]byte(" desc"))
	}
}
