package sql

import (
	"testing"
)

func Test_AlterTable(t *testing.T) {
	alter := &AlterTable{
		Name: "test1",
		Changes: []Part{
			DropColumn{Name: "col1"},
			RenameColumn{Name: "col1", To: "col2"},
			AddColumn{Column: Column{Name: "c3", Type: COLUMN_TYPE_TEXT, Nullable: false, Default: "def-1"}},
			RenameTable{To: "test2"},
		},
	}
	assertSQL(t, alter, `alter table test1 drop column col1;
alter table test1 rename column col1 to col2;
alter table test1 add column c3 text not null default('def-1');
alter table test1 rename to test2;
`)
}
