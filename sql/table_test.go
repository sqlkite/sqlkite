package sql

import (
	"testing"

	"src.goblgobl.com/sqlkite/data"
	"src.goblgobl.com/tests/assert"
	"src.goblgobl.com/utils/buffer"
)

func Test_CreateTable_Without_Defauls(t *testing.T) {
	buffer := buffer.New(256, 256)
	CreateTable(data.Table{
		Name: "tab1",
		Columns: []data.Column{
			data.Column{Name: "c1", Type: data.COLUMN_TYPE_TEXT, Nullable: true},
			data.Column{Name: "c2", Type: data.COLUMN_TYPE_INT, Nullable: false},
			data.Column{Name: "c3", Type: data.COLUMN_TYPE_REAL, Nullable: false},
			data.Column{Name: "c4", Type: data.COLUMN_TYPE_BLOB, Nullable: true},
		},
	}, buffer)

	assert.Equal(t, buffer.MustString(), `create table tab1(
	c1 text null,
	c2 int not null,
	c3 real not null,
	c4 blob null
)`)
}

func Test_CreateTable_With_Defauls(t *testing.T) {
	buffer := buffer.New(256, 256)
	CreateTable(data.Table{
		Name: "tab1",
		Columns: []data.Column{
			data.Column{Name: "c1", Type: data.COLUMN_TYPE_TEXT, Nullable: false, Default: "def-1"},
			data.Column{Name: "c2", Type: data.COLUMN_TYPE_INT, Nullable: true, Default: 2},
			data.Column{Name: "c3", Type: data.COLUMN_TYPE_REAL, Nullable: true, Default: 9000.1},
			data.Column{Name: "c4", Type: data.COLUMN_TYPE_BLOB, Nullable: false, Default: []byte("xyz123")},
		},
	}, buffer)

	assert.Equal(t, buffer.MustString(), `create table tab1(
	c1 text not null default('def-1'),
	c2 int null default(2),
	c3 real null default(9000.1),
	c4 blob not null default(x'78797a313233')
)`)
}
