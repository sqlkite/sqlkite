package sql

import (
	"testing"

	"src.goblgobl.com/tests/assert"
	"src.goblgobl.com/utils/buffer"
	"src.sqlkite.com/sqlkite/data"
)

func Test_TableAccessCreateTrigger_Without_When(t *testing.T) {
	buffer := buffer.New(256, 256)
	TableAccessCreateTrigger("tab1", "insert", &data.MutateTableAccess{Trigger: "select 1 ;"}, buffer)
	assert.Equal(t, buffer.MustString(), `create trigger sqlkite_row_access_tab1_insert
before insert on tab1 for each row
begin
 select 1 ;
end`)
}

func Test_TableAccessCreateTrigger_With_When(t *testing.T) {
	buffer := buffer.New(256, 256)
	TableAccessCreateTrigger("tab2", "update", &data.MutateTableAccess{When: "sqlkite_user_role() != 'admin'", Trigger: "select 1"}, buffer)
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
