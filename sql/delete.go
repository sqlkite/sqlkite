package sql

import (
	"strconv"

	"src.goblgobl.com/utils/buffer"
	"src.goblgobl.com/utils/optional"
)

type Delete struct {
	From       TableName
	Where      Condition
	Parameters []any
	OrderBy    []OrderBy
	Returning  []DataField
	Limit      optional.Int
	Offset     optional.Int
}

func (d Delete) Values() []any {
	return d.Parameters
}

func (d Delete) Write(b *buffer.Buffer) {
	b.Write([]byte("delete from "))
	d.From.Write(b)

	if where := d.Where; !where.Empty() {
		b.Write([]byte("\nwhere "))
		where.Write(b)
	}

	if returning := d.Returning; len(returning) > 0 {
		b.Write([]byte("\nreturning json_object("))
		for _, column := range returning {
			column.WriteAsJsonObject(b)
			b.Write([]byte(", "))
		}
		b.Truncate(2)
		b.WriteByte(')')
	}

	if orderBy := d.OrderBy; len(orderBy) > 0 {
		b.Write([]byte("\norder by "))
		for _, orderBy := range orderBy {
			orderBy.Write(b)
			b.WriteByte(',')
		}
		b.Truncate(1)
	}

	if limit := d.Limit; limit.Exists {
		b.Write([]byte("\nlimit "))
		b.WriteString(strconv.Itoa(limit.Value))
	}
	if offset := d.Offset; offset.Exists {
		b.Write([]byte("\noffset "))
		b.WriteString(strconv.Itoa(offset.Value))
	}
}
