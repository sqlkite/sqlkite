package data

import (
	"testing"

	"src.goblgobl.com/tests/assert"
)

func Test_Table_Column(t *testing.T) {
	table := Table{
		Columns: []Column{
			BuildColumn().Name("c1").Column(),
			BuildColumn().Name("c2").Column(),
		},
	}

	c, ok := table.Column("c1")
	assert.True(t, ok)
	assert.Equal(t, c.Name, "c1")

	c, ok = table.Column("c2")
	assert.True(t, ok)
	assert.Equal(t, c.Name, "c2")

	// SQLite isn't case sensitive, but we are. That's "ok", because we expect
	// input to be lowercased in our http handlers.
	_, ok = table.Column("C1")
	assert.False(t, ok)

	_, ok = table.Column("c3")
	assert.False(t, ok)
}
