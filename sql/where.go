package sql

import "src.goblgobl.com/utils/buffer"

var (
	EmptyCondition = Condition{}
)

type Predicate struct {
	Left  DataField `json:"left"`
	Op    []byte    `json:"op"`
	Right DataField `json:"right"`
}

func (p Predicate) Write(b *buffer.Buffer) {
	p.Left.Write(b)
	b.Write(p.Op)
	p.Right.Write(b)
}

type Condition struct {
	// can be nested conditions and/or predicates
	Parts []Part `json:"parts"`

	// len(Logical) == len(Parts)-1
	Logicals []LogicalOperator `json:"logicals"`
}

func (c Condition) Empty() bool {
	return len(c.Parts) == 0
}

func (c Condition) Write(b *buffer.Buffer) {
	parts := c.Parts
	logicals := c.Logicals

	if len(parts) == 0 {
		b.Write([]byte("true"))
		return
	}

	b.WriteByte('(')
	parts[0].Write(b)

	for i, part := range parts[1:] {
		if logicals[i] == LOGICAL_AND {
			b.Write([]byte(" and "))
		} else {
			b.Write([]byte(" or "))
		}
		part.Write(b)
	}
	b.WriteByte(')')
}
