package sql

import (
	"strconv"

	"src.goblgobl.com/utils/buffer"
	"src.goblgobl.com/utils/optional"
)

type Select struct {
	CTEs       []CTE
	Columns    []DataField
	Froms      []JoinableFrom
	Where      Condition
	Parameters []any
	Limit      int
	Offset     optional.Int
	OrderBy    []OrderBy
}

func (s Select) Values() []any {
	return s.Parameters
}

// we trust that our caller knows that index < len(s.Froms)
func (s *Select) CTE(index int, name string, cte string) {
	s.Froms[index].Table.Name = name
	s.CTEs = append(s.CTEs, CTE{Name: name, CTE: cte})
}

func (s Select) Write(b *buffer.Buffer) {
	if ctes := s.CTEs; len(ctes) > 0 {
		b.Write([]byte("with "))
		ctes[0].Write(b)
		for _, cte := range ctes[1:] {
			b.Write([]byte(", "))
			cte.Write(b)
		}
		b.WriteByte(' ')
	}

	b.Write([]byte("select json_object("))
	for _, column := range s.Columns {
		column.WriteAsJsonObject(b)
		b.Write([]byte(", "))
	}
	b.Truncate(2)

	froms := s.Froms
	b.Write([]byte(")\nfrom "))
	for _, from := range froms {
		from.Write(b)
		b.Write([]byte("\n "))
	}
	b.Truncate(2)

	if where := s.Where; !where.Empty() {
		b.Write([]byte("\nwhere "))
		s.Where.Write(b)
	}

	if orderBy := s.OrderBy; len(orderBy) > 0 {
		b.Write([]byte("\norder by "))
		for _, orderBy := range orderBy {
			orderBy.Write(b)
			b.WriteByte(',')
		}
		b.Truncate(1)
	}

	b.Write([]byte("\nlimit "))
	b.WriteString(strconv.Itoa(s.Limit))
	if offset := s.Offset; offset.Exists {
		b.Write([]byte("\noffset "))
		b.WriteString(strconv.Itoa(offset.Value))
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
