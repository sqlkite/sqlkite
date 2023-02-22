package sql

import (
	"strconv"

	"src.goblgobl.com/utils/buffer"
	"src.goblgobl.com/utils/optional"
)

type Update struct {
	Target     TableName
	Set        []UpdateSet
	Froms      []JoinableFrom
	OrderBy    []OrderBy
	Parameters []any
	Returning  []DataField
	Where      Condition
	Limit      optional.Int
	Offset     optional.Int
}

func (u Update) Values() []any {
	return u.Parameters
}

func (u Update) Write(b *buffer.Buffer) {
	b.Write([]byte("update "))
	u.Target.Write(b)
	b.Write([]byte(" set\n "))
	for _, set := range u.Set {
		set.Write(b)
		b.Write([]byte(",\n "))
	}
	b.Truncate(3)

	if froms := u.Froms; len(froms) > 0 {
		b.Write([]byte("\nfrom "))
		for _, from := range froms {
			from.Write(b)
			b.Write([]byte("\n "))
		}
		b.Truncate(2)
	}

	if where := u.Where; !where.Empty() {
		b.Write([]byte("\nwhere "))
		where.Write(b)
	}

	if returning := u.Returning; len(returning) > 0 {
		b.Write([]byte("\nreturning json_object("))
		for _, column := range returning {
			column.WriteAsJsonObject(b)
			b.Write([]byte(", "))
		}
		b.Truncate(2)
		b.WriteByte(')')
	}

	if orderBy := u.OrderBy; len(orderBy) > 0 {
		b.Write([]byte("\norder by "))
		for _, orderBy := range orderBy {
			orderBy.Write(b)
			b.WriteByte(',')
		}
		b.Truncate(1)
	}

	if limit := u.Limit; limit.Exists {
		b.Write([]byte("\nlimit "))
		b.WriteString(strconv.Itoa(limit.Value))
	}
	if offset := u.Offset; offset.Exists {
		b.Write([]byte("\noffset "))
		b.WriteString(strconv.Itoa(offset.Value))
	}
}

type UpdateSet struct {
	Column string
	Value  DataField
}

func (u UpdateSet) Write(b *buffer.Buffer) {
	b.WriteUnsafe(u.Column)
	b.Write([]byte(" = "))
	u.Value.Write(b)
}
