package sql

import (
	"strconv"

	"src.goblgobl.com/utils/buffer"
	"src.goblgobl.com/utils/optional"
)

type Delete struct {
	From       string
	Where      Condition
	Parameters []any
	Limit      optional.Value[int]
}

func (d Delete) Write(b *buffer.Buffer) {
	b.Write([]byte("delete from "))
	b.WriteUnsafe(d.From)

	if where := d.Where; !where.Empty() {
		b.Write([]byte("\nwhere "))
		where.Write(b)
	}

	if limit := d.Limit; limit.Exists {
		b.Write([]byte("\nlimit "))
		b.WriteString(strconv.Itoa(limit.Value))
	}
}
