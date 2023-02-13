package sqlkite

import (
	"testing"

	"src.goblgobl.com/tests/assert"
	"src.goblgobl.com/utils/buffer"
	"src.goblgobl.com/utils/optional"
	"src.sqlkite.com/sqlkite/sql"
	"src.sqlkite.com/sqlkite/tests"
)

func Test_Table_Column(t *testing.T) {
	table := Table{
		Columns: []Column{
			Column{Name: "c1"},
			Column{Name: "c2"},
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

func Test_CreateTable_Without_Defaults(t *testing.T) {
	table := &Table{
		Name: "tab1",
		Columns: []Column{
			Column{Name: "c1", Type: COLUMN_TYPE_TEXT, Nullable: true},
			Column{Name: "c2", Type: COLUMN_TYPE_INT, Nullable: false},
			Column{Name: "c3", Type: COLUMN_TYPE_REAL, Nullable: false},
			Column{Name: "c4", Type: COLUMN_TYPE_BLOB, Nullable: true},
		},
	}

	tests.AssertSQL(t, table, `create table tab1(
	c1 text null,
	c2 int not null,
	c3 real not null,
	c4 blob null
)`)
}

func Test_CreateTable_With_Defaults(t *testing.T) {
	table := &Table{
		Name:       "tab1",
		PrimaryKey: []string{"c1"},
		Columns: []Column{
			Column{Name: "c1", Type: COLUMN_TYPE_TEXT, Nullable: false, Default: "def-1"},
			Column{Name: "c2", Type: COLUMN_TYPE_INT, Nullable: true, Default: 2},
			Column{Name: "c3", Type: COLUMN_TYPE_REAL, Nullable: true, Default: 9000.1},
			Column{Name: "c4", Type: COLUMN_TYPE_BLOB, Nullable: false, Default: []byte("xyz123")},
		},
	}

	tests.AssertSQL(t, table, `create table tab1(
	c1 text not null default('def-1'),
	c2 int null default(2),
	c3 real null default(9000.1),
	c4 blob not null default(x'78797a313233'),
	primary key (c1)
)`)
}

func Test_CreateTable_With_Multiple_PK(t *testing.T) {
	table := &Table{
		Name:       "tab1",
		PrimaryKey: []string{"c1", "c2"},
	}

	tests.AssertSQL(t, table, `create table tab1(
	primary key (c1,c2)
)`)
}

func Test_CreateTable_AutoIncrement_Strict(t *testing.T) {
	table := &Table{
		Name:       "tab1",
		PrimaryKey: []string{"id"},
		Columns: []Column{
			Column{Name: "id", Type: COLUMN_TYPE_INT, Extended: &ColumnExtended{AutoIncrement: optional.New(AUTO_INCREMENT_TYPE_STRICT)}},
			Column{Name: "name", Type: COLUMN_TYPE_TEXT},
		},
	}

	tests.AssertSQL(t, table, `create table tab1(
	id integer primary key autoincrement not null,
	name text not null
)`)
}

func Test_CreateTable_AutoIncrement_Reuse(t *testing.T) {
	table := &Table{
		Name:       "tab1",
		PrimaryKey: []string{"id"},
		Columns: []Column{
			Column{Name: "id", Type: COLUMN_TYPE_INT, Extended: &ColumnExtended{AutoIncrement: optional.New(AUTO_INCREMENT_TYPE_REUSE)}},
			Column{Name: "name", Type: COLUMN_TYPE_TEXT},
		},
	}

	tests.AssertSQL(t, table, `create table tab1(
	id integer primary key not null,
	name text not null
)`)
}

func Test_TableAccessCreateTrigger_Without_When(t *testing.T) {
	buffer := buffer.New(256, 256)
	access := NewTableAccessMutate("tab1", TABLE_ACCESS_MUTATE_INSERT, "select 1 ;", "")
	access.WriteCreate(buffer)
	assert.Equal(t, buffer.MustString(), `create trigger sqlkite_ra_tab1_i
before insert on tab1 for each row
begin
 select 1 ;
end`)
}

func Test_TableAccessCreateTrigger_With_When(t *testing.T) {
	buffer := buffer.New(256, 256)
	access := NewTableAccessMutate("tab2", TABLE_ACCESS_MUTATE_UPDATE, "select 1", "sqlkite_user_role() != 'admin'")
	access.WriteCreate(buffer)
	assert.Equal(t, buffer.MustString(), `create trigger sqlkite_ra_tab2_u
before update on tab2 for each row
when (sqlkite_user_role() != 'admin')
begin
 select 1;
end`)
}

func Test_TableAccessDropTrigger_With_When(t *testing.T) {
	buffer := buffer.New(256, 256)
	access := NewTableAccessMutate("tab3", TABLE_ACCESS_MUTATE_DELETE, "", "")
	access.WriteDrop(buffer)
	assert.Equal(t, buffer.MustString(), `drop trigger if exists sqlkite_ra_tab3_d`)
}

func Test_TableAlter(t *testing.T) {
	alter := &TableAlter{
		Name: "test1",
		Changes: []sql.Part{
			TableAlterDropColumn{Name: "col1"},
			TableAlterRenameColumn{Name: "col1", To: "col2"},
			TableAlterAddColumn{Column: Column{Name: "c3", Type: COLUMN_TYPE_TEXT, Nullable: false, Default: "def-1"}},
			TableAlterRename{To: "test2"},
		},
	}
	tests.AssertSQL(t, alter, `alter table test1 drop column col1;
alter table test1 rename column col1 to col2;
alter table test1 add column c3 text not null default('def-1');
alter table test1 rename to test2;
`)
}
