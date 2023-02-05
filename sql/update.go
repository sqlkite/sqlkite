package sql

import (
	"strconv"

	"src.goblgobl.com/utils/buffer"
	"src.goblgobl.com/utils/optional"
)

type Update struct {
	Target     string
	Set        []UpdateSet
	Froms      []SelectFrom
	Where      Condition
	Limit      optional.Value[int]
	Parameters []any
}

func (u Update) Write(b *buffer.Buffer) {
	b.Write([]byte("update "))
	b.WriteUnsafe(u.Target)
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

	if limit := u.Limit; limit.Exists {
		b.Write([]byte("\nlimit "))
		b.WriteString(strconv.Itoa(limit.Value))
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
