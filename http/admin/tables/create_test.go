package tables

import (
	"testing"

	"src.goblgobl.com/tests/assert"
	"src.goblgobl.com/tests/request"
	"src.goblgobl.com/utils"
	"src.sqlkite.com/sqlkite"
	"src.sqlkite.com/sqlkite/codes"
	"src.sqlkite.com/sqlkite/sql"
	"src.sqlkite.com/sqlkite/tests"
)

func Test_Create_InvalidBody(t *testing.T) {
	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body("nope").
		Post(Create).
		ExpectInvalid(utils.RES_INVALID_JSON_PAYLOAD)
}

func Test_Create_InvalidData(t *testing.T) {
	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Post(Create).
		ExpectValidation("name", utils.VAL_REQUIRED, "columns", utils.VAL_REQUIRED).ExpectNoValidation("max_update_count", "max_delete_count")

	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body(map[string]any{
			"name":             "4",
			"columns":          []any{},
			"access":           9,
			"max_update_count": "two",
			"max_delete_count": "three",
		}).
		Post(Create).
		ExpectValidation("name", utils.VAL_STRING_PATTERN, "columns", utils.VAL_ARRAY_MIN_LENGTH, "access", utils.VAL_OBJECT_TYPE, "max_update_count", utils.VAL_INT_TYPE, "max_delete_count", utils.VAL_INT_TYPE)

	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body(map[string]any{
			"name":             "h@t",
			"columns":          []any{map[string]any{}},
			"access":           map[string]any{"select": 32, "insert": "no", "update": true, "delete": []any{}},
			"max_update_count": -4,
			"max_delete_count": -5,
		}).
		Post(Create).
		ExpectValidation(
			"max_update_count", utils.VAL_INT_MIN,
			"max_delete_count", utils.VAL_INT_MIN,
			"name", utils.VAL_STRING_PATTERN, "columns.0.name", utils.VAL_REQUIRED, "columns.0.type", utils.VAL_REQUIRED, "columns.0.nullable", utils.VAL_REQUIRED,
			"access.select", utils.VAL_STRING_TYPE, "access.insert", utils.VAL_OBJECT_TYPE, "access.update", utils.VAL_OBJECT_TYPE, "access.delete", utils.VAL_OBJECT_TYPE).
		ExpectNoValidation("columns.0.default")

	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body(map[string]any{
			"name": "h@t",
			"columns": []any{
				map[string]any{"name": "l@w", "type": "float64", "nullable": 1},
			},
			"access": map[string]any{
				"insert": map[string]any{"when": 32, "trigger": true},
			},
		}).
		Post(Create).
		ExpectValidation(
			"name", utils.VAL_STRING_PATTERN, "columns.0.name", utils.VAL_STRING_PATTERN, "columns.0.type", utils.VAL_STRING_CHOICE, "columns.0.nullable", utils.VAL_BOOL_TYPE,
			"access.insert.when", utils.VAL_STRING_TYPE, "access.insert.trigger", utils.VAL_STRING_TYPE)

	// table name cannot start with sqlkite
	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body(map[string]any{
			"name": "sqlKite_hello",
			"columns": []any{
				map[string]any{"name": "low", "type": "float64", "nullable": true},
			},
		}).
		Post(Create).
		ExpectValidation("name", codes.VAL_RESERVED_TABLE_NAME)
}

func Test_Create_InvalidDefault(t *testing.T) {
	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body(map[string]any{
			"name": "tab1",
			"columns": []any{
				map[string]any{"name": "c1", "type": "text", "nullable": true, "default": true},
				map[string]any{"name": "c2", "type": "int", "nullable": true, "default": 123.3},
				map[string]any{"name": "c3", "type": "real", "nullable": true, "default": "nope"},
				map[string]any{"name": "c4", "type": "blob", "nullable": true, "default": "zA-("},
			},
		}).
		Post(Create).
		ExpectValidation("columns.0.default", utils.VAL_STRING_TYPE, "columns.1.default", utils.VAL_INT_TYPE, "columns.2.default", utils.VAL_FLOAT_TYPE, "columns.3.default", codes.VAL_NON_BASE64_COLUMN_DEFAULT)
}

func Test_Create_TooManyTables(t *testing.T) {
	project, _ := sqlkite.Projects.Get(tests.Factory.LimitedId)
	request.ReqT(t, project.Env()).
		Body(map[string]any{
			"name": "an_extra_table",
			"columns": []any{
				map[string]any{"name": "c1", "type": "text", "nullable": false},
			},
		}).
		Post(Create).
		ExpectValidation("", codes.VAL_TOO_MANY_TABLES)
}

func Test_Create_Success_Defaults_WithAccessControl(t *testing.T) {
	id := tests.Factory.DynamicId()
	project, _ := sqlkite.Projects.Get(id)
	request.ReqT(t, project.Env()).
		Body(map[string]any{
			"name": "test_create_success_defaults",
			"columns": []any{
				map[string]any{"name": "C1", "type": "text", "nullable": true, "default": "a"},
				map[string]any{"name": "c2", "type": "int", "nullable": true, "default": 32},
				map[string]any{"name": "C3", "type": "real", "nullable": true, "default": 9000.1},
				map[string]any{"name": "c4", "type": "blob", "nullable": true, "default": "b3ZlcjkwMDA="},
			},
			"access": map[string]any{
				"select": "select * from a_table where user_id = sqlkite_user_id()",
				"insert": map[string]any{"when": "1=1", "trigger": "select 1"},
				"update": map[string]any{"when": " 2=2\n", "trigger": "select 2 ;"},
				"delete": map[string]any{"when": "3=3", "trigger": " select 3  "},
			},
		}).
		Post(Create).
		OK()

	// reload the project, because in-memory projects are immutable
	project, _ = sqlkite.Projects.Get(id)
	table := project.Table("test_create_success_defaults")
	assert.Equal(t, table.Name, "test_create_success_defaults")
	assert.Equal(t, len(table.Columns), 4)

	assert.False(t, table.MaxUpdateCount.Exists)
	assert.False(t, table.MaxDeleteCount.Exists)

	assert.Equal(t, table.Columns[0].Name, "C1")
	assert.Equal(t, table.Columns[0].Default.(string), "a")
	assert.Equal(t, table.Columns[0].Nullable, true)
	assert.Equal(t, table.Columns[0].Type, sql.COLUMN_TYPE_TEXT)

	assert.Equal(t, table.Columns[1].Name, "c2")
	assert.Equal(t, table.Columns[1].Default.(float64), 32)
	assert.Equal(t, table.Columns[1].Nullable, true)
	assert.Equal(t, table.Columns[1].Type, sql.COLUMN_TYPE_INT)

	assert.Equal(t, table.Columns[2].Name, "C3")
	assert.Equal(t, table.Columns[2].Default.(float64), 9000.1)
	assert.Equal(t, table.Columns[2].Nullable, true)
	assert.Equal(t, table.Columns[2].Type, sql.COLUMN_TYPE_REAL)

	assert.Equal(t, table.Columns[3].Name, "c4")
	assert.Equal(t, string(table.Columns[3].Default.([]byte)), "over9000")
	assert.Equal(t, table.Columns[3].Nullable, true)
	assert.Equal(t, table.Columns[3].Type, sql.COLUMN_TYPE_BLOB)

	assert.Equal(t, table.Access.Select.Name, "sqlkite_cte_test_create_success_defaults")
	assert.Equal(t, table.Access.Select.CTE, "select * from a_table where user_id = sqlkite_user_id()")

	assert.Equal(t, table.Access.Insert.When, "1=1")
	assert.Equal(t, table.Access.Insert.Trigger, "select 1")
	assert.Equal(t, table.Access.Update.When, "2=2")
	assert.Equal(t, table.Access.Update.Trigger, "select 2 ;")
	assert.Equal(t, table.Access.Delete.When, "3=3")
	assert.Equal(t, table.Access.Delete.Trigger, "select 3")

	insertAccessControl := tests.SqliteMaster(project, "sqlkite_row_access_test_create_success_defaults_insert", "test_create_success_defaults")
	assert.Equal(t, insertAccessControl, `CREATE TRIGGER sqlkite_row_access_test_create_success_defaults_insert
before insert on test_create_success_defaults for each row
when (1=1)
begin
 select 1;
end`)

	updateAccessControl := tests.SqliteMaster(project, "sqlkite_row_access_test_create_success_defaults_update", "test_create_success_defaults")
	assert.Equal(t, updateAccessControl, `CREATE TRIGGER sqlkite_row_access_test_create_success_defaults_update
before update on test_create_success_defaults for each row
when (2=2)
begin
 select 2 ;
end`)

	deleteAccessControl := tests.SqliteMaster(project, "sqlkite_row_access_test_create_success_defaults_delete", "test_create_success_defaults")
	assert.Equal(t, deleteAccessControl, `CREATE TRIGGER sqlkite_row_access_test_create_success_defaults_delete
before delete on test_create_success_defaults for each row
when (3=3)
begin
 select 3;
end`)
}

func Test_Create_Success_NoDefaults_NoAccessControl(t *testing.T) {
	id := tests.Factory.DynamicId()
	project, _ := sqlkite.Projects.Get(id)
	request.ReqT(t, project.Env()).
		Body(map[string]any{
			"name":             "test_create_success_defaults",
			"max_update_count": 2,
			"max_delete_count": 3,
			"columns": []any{
				map[string]any{"name": "c1", "type": "text", "nullable": false},
				map[string]any{"name": "c2", "type": "int", "nullable": false},
				map[string]any{"name": "c3", "type": "real", "nullable": false},
				map[string]any{"name": "c4", "type": "blob", "nullable": false},
			},
		}).
		Post(Create).
		OK()

	// reload the project, because in-memory projects are immutable
	project, _ = sqlkite.Projects.Get(id)
	table := project.Table("test_create_success_defaults")
	assert.Equal(t, table.Name, "test_create_success_defaults")

	assert.Equal(t, table.MaxUpdateCount.Value, 2)
	assert.Equal(t, table.MaxDeleteCount.Value, 3)

	assert.Equal(t, len(table.Columns), 4)
	assert.Equal(t, table.Columns[0].Name, "c1")
	assert.Nil(t, table.Columns[0].Default)
	assert.Equal(t, table.Columns[0].Nullable, false)
	assert.Equal(t, table.Columns[0].Type, sql.COLUMN_TYPE_TEXT)

	assert.Equal(t, table.Columns[1].Name, "c2")
	assert.Nil(t, table.Columns[1].Default)
	assert.Equal(t, table.Columns[1].Nullable, false)
	assert.Equal(t, table.Columns[1].Type, sql.COLUMN_TYPE_INT)

	assert.Equal(t, table.Columns[2].Name, "c3")
	assert.Nil(t, table.Columns[2].Default)
	assert.Equal(t, table.Columns[2].Nullable, false)
	assert.Equal(t, table.Columns[2].Type, sql.COLUMN_TYPE_REAL)

	assert.Equal(t, table.Columns[3].Name, "c4")
	assert.Nil(t, table.Columns[3].Default)
	assert.Equal(t, table.Columns[3].Nullable, false)
	assert.Equal(t, table.Columns[3].Type, sql.COLUMN_TYPE_BLOB)

	assert.Nil(t, table.Access.Select)
	assert.Nil(t, table.Access.Insert)
	assert.Nil(t, table.Access.Update)
	assert.Nil(t, table.Access.Delete)

	insertAccessControl := tests.SqliteMaster(project, "sqlkite_row_access_insert", "test_create_success_defaults")
	assert.Equal(t, insertAccessControl, "")

	updateAccessControl := tests.SqliteMaster(project, "sqlkite_row_access_update", "test_create_success_defaults")
	assert.Equal(t, updateAccessControl, "")

	deleteAccessControl := tests.SqliteMaster(project, "sqlkite_row_access_delete", "test_create_success_defaults")
	assert.Equal(t, deleteAccessControl, "")
}
