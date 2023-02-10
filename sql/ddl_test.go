package sql

import (
	"testing"

	"src.goblgobl.com/tests/assert"
	"src.goblgobl.com/utils/buffer"
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

func Test_CreateTable_Without_Defauls(t *testing.T) {
	buffer := buffer.New(256, 256)
	table := &Table{
		Name: "tab1",
		Columns: []Column{
			Column{Name: "c1", Type: COLUMN_TYPE_TEXT, Nullable: true},
			Column{Name: "c2", Type: COLUMN_TYPE_INT, Nullable: false},
			Column{Name: "c3", Type: COLUMN_TYPE_REAL, Nullable: false},
			Column{Name: "c4", Type: COLUMN_TYPE_BLOB, Nullable: true},
		},
	}
	table.Write(buffer)
	assert.Equal(t, buffer.MustString(), `create table tab1(
	c1 text null,
	c2 int not null,
	c3 real not null,
	c4 blob null
)`)
}

func Test_CreateTable_With_Defaults(t *testing.T) {
	buffer := buffer.New(256, 256)
	table := &Table{
		Name: "tab1",
		Columns: []Column{
			Column{Name: "c1", Type: COLUMN_TYPE_TEXT, Nullable: false, Default: "def-1"},
			Column{Name: "c2", Type: COLUMN_TYPE_INT, Nullable: true, Default: 2},
			Column{Name: "c3", Type: COLUMN_TYPE_REAL, Nullable: true, Default: 9000.1},
			Column{Name: "c4", Type: COLUMN_TYPE_BLOB, Nullable: false, Default: []byte("xyz123")},
		},
	}
	table.Write(buffer)

	assert.Equal(t, buffer.MustString(), `create table tab1(
	c1 text not null default('def-1'),
	c2 int null default(2),
	c3 real null default(9000.1),
	c4 blob not null default(x'78797a313233')
)`)
}

func Test_AlterTable(t *testing.T) {
	buffer := buffer.New(1024, 1024)
	alter := AlterTable{
		Name: "test1",
		Changes: []AlterTableChange{
			DropColumn{Name: "col1"},
			RenameColumn{Name: "col1", To: "col2"},
			AddColumn{Column: Column{Name: "c3", Type: COLUMN_TYPE_TEXT, Nullable: false, Default: "def-1"}},
			RenameTable{To: "test2"},
		},
	}
	alter.Write(buffer)
	assert.Equal(t, buffer.MustString(), `alter table test1 drop column col1;
alter table test1 rename column col1 to col2;
alter table test1 add column c3 text not null default('def-1');
alter table test1 rename to test2;
`)
}

func Test_TableAccessCreateTrigger_Without_When(t *testing.T) {
	buffer := buffer.New(256, 256)
	TableAccessCreateTrigger("tab1", "insert", &MutateTableAccess{Trigger: "select 1 ;"}, buffer)
	assert.Equal(t, buffer.MustString(), `create trigger sqlkite_row_access_tab1_insert
before insert on tab1 for each row
begin
 select 1 ;
end`)
}

func Test_TableAccessCreateTrigger_With_When(t *testing.T) {
	buffer := buffer.New(256, 256)
	TableAccessCreateTrigger("tab2", "update", &MutateTableAccess{When: "sqlkite_user_role() != 'admin'", Trigger: "select 1"}, buffer)
	assert.Equal(t, buffer.MustString(), `create trigger sqlkite_row_access_tab2_update
before update on tab2 for each row
when (sqlkite_user_role() != 'admin')
begin
 select 1;
end`)
}

func Test_TableAccessDropTrigger_With_When(t *testing.T) {
	buffer := buffer.New(256, 256)
	TableAccessDropTrigger("tab3", "delete", buffer)
	assert.Equal(t, buffer.MustString(), `drop trigger if exists sqlkite_row_access_tab3_delete`)
}
