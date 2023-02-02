package sql

import (
	"testing"

	"src.goblgobl.com/sqlkite/data"
	"src.goblgobl.com/tests/assert"
	"src.goblgobl.com/utils/buffer"
)

func Test_TableAccessTrigger_Without_When(t *testing.T) {
	buffer := buffer.New(256, 256)
	TableAccessTrigger("tab1", "insert", &data.MutateTableAccess{Trigger: "select 1 ;"}, buffer)
	assert.Equal(t, buffer.MustString(), `create trigger sqlkite_row_access_insert
before insert on tab1 for each row
begin
 select 1 ;
end`)
}

func Test_TableAccessTrigger_With_When(t *testing.T) {
	buffer := buffer.New(256, 256)
	TableAccessTrigger("tab2", "update", &data.MutateTableAccess{When: "sqlkite_user_role() != 'admin'", Trigger: "select 1"}, buffer)
	assert.Equal(t, buffer.MustString(), `create trigger sqlkite_row_access_update
before update on tab2 for each row
when (sqlkite_user_role() != 'admin')
begin
 select 1;
end`)
}
