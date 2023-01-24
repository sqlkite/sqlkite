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
		}).
		Post(Create).
		ExpectValidation("name", 1004, "columns", 1012)

	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body(map[string]any{
			"name": "h@t",
			"columns": []any{
				map[string]any{},
			},
		}).
		Post(Create).
		ExpectValidation("name", 1004, "columns.0.name", 1001, "columns.0.type", 1001, "columns.0.nullable", 1001).
		ExpectNoValidation("columns.0.default")

	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body(map[string]any{
			"name": "h@t",
			"columns": []any{
				map[string]any{"name": "l@w", "type": "float64", "nullable": 1},
			},
		}).
		Post(Create).
		ExpectValidation("name", 1004, "columns.0.name", 1004, "columns.0.type", 1015, "columns.0.nullable", 1009)
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

func Test_Create_Success_Defaults(t *testing.T) {
	id := tests.Factory.DynamicId()
	project, _ := sqlkite.Projects.Get(id)
	request.ReqT(t, project.Env()).
		Body(map[string]any{
			"name": "Test_Create_Success_Defaults",
			"columns": []any{
				map[string]any{"name": "c1", "type": "text", "nullable": true, "default": "a"},
				map[string]any{"name": "c2", "type": "int", "nullable": true, "default": 32},
				map[string]any{"name": "c3", "type": "real", "nullable": true, "default": 9000.1},
				map[string]any{"name": "c4", "type": "blob", "nullable": true, "default": "b3ZlcjkwMDA="},
			},
		}).
		Post(Create).
		OK()

	// reload the project, because in-memory projects are immutable
	project, _ = sqlkite.Projects.Get(id)
	table, exists := project.Table("Test_Create_Success_Defaults")
	assert.True(t, exists)
	assert.Equal(t, table.Name, "Test_Create_Success_Defaults")
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
}
