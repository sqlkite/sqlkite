package tables

import (
	"testing"

	"src.goblgobl.com/sqlkite"
	"src.goblgobl.com/sqlkite/data"
	"src.goblgobl.com/sqlkite/tests"
	"src.goblgobl.com/tests/assert"
	"src.goblgobl.com/tests/request"
)

func Test_Create_InvalidBody(t *testing.T) {
	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body("nope").
		Post(Create).
		ExpectInvalid(2003)
}

func Test_Create_InvalidData(t *testing.T) {
	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Post(Create).
		ExpectValidation("name", 1001, "columns", 1001)

	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body(map[string]any{
			"name":    "4",
			"columns": []any{},
			"access":  9,
		}).
		Post(Create).
		ExpectValidation("name", 1004, "columns", 1012, "access", 1022)

	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body(map[string]any{
			"name":    "h@t",
			"columns": []any{map[string]any{}},
			"access":  map[string]any{"select": 32, "insert": "no", "update": true, "delete": []any{}},
		}).
		Post(Create).
		ExpectValidation(
			"name", 1004, "columns.0.name", 1001, "columns.0.type", 1001, "columns.0.nullable", 1001,
			"access.select", 1002, "access.insert", 1022, "access.update", 1022, "access.delete", 1022).
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
			"name", 1004, "columns.0.name", 1004, "columns.0.type", 1015, "columns.0.nullable", 1009,
			"access.insert.when", 1002, "access.insert.trigger", 1002)

	// table name cannot start with sqlkite
	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body(map[string]any{
			"name": "sqlKite_hello",
			"columns": []any{
				map[string]any{"name": "low", "type": "float64", "nullable": true},
			},
		}).
		Post(Create).
		ExpectValidation("name", 302_034)
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
		ExpectValidation("columns.0.default", 1002, "columns.1.default", 1005, "columns.2.default", 1016, "columns.3.default", 301_017)
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
		ExpectValidation("", 301032)
}

func Test_Create_Success_Defaults_WithAccessControl(t *testing.T) {
	id := tests.Factory.DynamicId()
	project, _ := sqlkite.Projects.Get(id)
	request.ReqT(t, project.Env()).
		Body(map[string]any{
			"name": "Test_Create_Success_Defaults",
			"columns": []any{
				map[string]any{"name": "C1", "type": "text", "nullable": true, "default": "a"},
				map[string]any{"name": "c2", "type": "int", "nullable": true, "default": 32},
				map[string]any{"name": "C3", "type": "real", "nullable": true, "default": 9000.1},
				map[string]any{"name": "c4", "type": "blob", "nullable": true, "default": "b3ZlcjkwMDA="},
			},
			"access": map[string]any{
				"select": "select * from a_table where user_id = sqlkite_user_id()",
				"insert": map[string]any{"when": "1=1", "trigger": "select 1;"},
				"update": map[string]any{"when": "2=2", "trigger": "select 2;"},
				"delete": map[string]any{"when": "3=3", "trigger": "select 3;"},
			},
		}).
		Post(Create).
		OK()

	// reload the project, because in-memory projects are immutable
	project, _ = sqlkite.Projects.Get(id)
	table, exists := project.Table("test_create_success_defaults")
	assert.True(t, exists)
	assert.Equal(t, table.Name, "test_create_success_defaults")
	assert.Equal(t, len(table.Columns), 4)

	assert.Equal(t, table.Columns[0].Name, "c1")
	assert.Equal(t, table.Columns[0].Default.(string), "a")
	assert.Equal(t, table.Columns[0].Nullable, true)
	assert.Equal(t, table.Columns[0].Type, data.COLUMN_TYPE_TEXT)

	assert.Equal(t, table.Columns[1].Name, "c2")
	assert.Equal(t, table.Columns[1].Default.(float64), 32)
	assert.Equal(t, table.Columns[1].Nullable, true)
	assert.Equal(t, table.Columns[1].Type, data.COLUMN_TYPE_INT)

	assert.Equal(t, table.Columns[2].Name, "c3")
	assert.Equal(t, table.Columns[2].Default.(float64), 9000.1)
	assert.Equal(t, table.Columns[2].Nullable, true)
	assert.Equal(t, table.Columns[2].Type, data.COLUMN_TYPE_REAL)

	assert.Equal(t, table.Columns[3].Name, "c4")
	assert.Equal(t, string(table.Columns[3].Default.([]byte)), "over9000")
	assert.Equal(t, table.Columns[3].Nullable, true)
	assert.Equal(t, table.Columns[3].Type, data.COLUMN_TYPE_BLOB)

	assert.Equal(t, table.Access.Select.Name, "sqlkite_cte_test_create_success_defaults")
	assert.Equal(t, table.Access.Select.CTE, "select * from a_table where user_id = sqlkite_user_id()")

	assert.Equal(t, table.Access.Insert.When, "1=1")
	assert.Equal(t, table.Access.Insert.Trigger, "select 1;")
	assert.Equal(t, table.Access.Update.When, "2=2")
	assert.Equal(t, table.Access.Update.Trigger, "select 2;")
	assert.Equal(t, table.Access.Delete.When, "3=3")
	assert.Equal(t, table.Access.Delete.Trigger, "select 3;")
}

func Test_Create_Success_NoDefaults_NoAccessControl(t *testing.T) {
	id := tests.Factory.DynamicId()
	project, _ := sqlkite.Projects.Get(id)
	request.ReqT(t, project.Env()).
		Body(map[string]any{
			"name": "Test_Create_Success_Defaults",
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
	table, exists := project.Table("test_create_success_defaults")
	assert.True(t, exists)
	assert.Equal(t, table.Name, "test_create_success_defaults")
	assert.Equal(t, len(table.Columns), 4)

	assert.Equal(t, table.Columns[0].Name, "c1")
	assert.Nil(t, table.Columns[0].Default)
	assert.Equal(t, table.Columns[0].Nullable, false)
	assert.Equal(t, table.Columns[0].Type, data.COLUMN_TYPE_TEXT)

	assert.Equal(t, table.Columns[1].Name, "c2")
	assert.Nil(t, table.Columns[1].Default)
	assert.Equal(t, table.Columns[1].Nullable, false)
	assert.Equal(t, table.Columns[1].Type, data.COLUMN_TYPE_INT)

	assert.Equal(t, table.Columns[2].Name, "c3")
	assert.Nil(t, table.Columns[2].Default)
	assert.Equal(t, table.Columns[2].Nullable, false)
	assert.Equal(t, table.Columns[2].Type, data.COLUMN_TYPE_REAL)

	assert.Equal(t, table.Columns[3].Name, "c4")
	assert.Nil(t, table.Columns[3].Default)
	assert.Equal(t, table.Columns[3].Nullable, false)
	assert.Equal(t, table.Columns[3].Type, data.COLUMN_TYPE_BLOB)

	assert.Nil(t, table.Access.Select)
}
