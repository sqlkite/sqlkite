package tables

import (
	"testing"

	"src.goblgobl.com/sqlkite"
	"src.goblgobl.com/sqlkite/data"
	"src.goblgobl.com/sqlkite/tests"
	"src.goblgobl.com/tests/assert"
	"src.goblgobl.com/tests/request"
)

func Test_Update_InvalidBody(t *testing.T) {
	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body("nope").
		Put(Update).
		ExpectInvalid(2003)
}

func Test_Update_InvalidData(t *testing.T) {
	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body(map[string]any{
			"changes": "4",
		}).
		Put(Update).
		ExpectValidation("changes", 1011)

	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body(map[string]any{
			"changes": []any{
				4,
				map[string]any{},
				map[string]any{"type": "nope"},
			},
		}).
		Put(Update).
		ExpectValidation("changes.0", 1022, "changes.1.type", 1001, "changes.2.type", 1015)

	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body(map[string]any{
			"changes": []any{
				map[string]any{"type": "RENAME"},
				map[string]any{"type": "rename", "to": 32},
				map[string]any{"type": "Rename", "to": "***"},
				map[string]any{"type": "drop column"},
				map[string]any{"type": "drop column", "name": 32},
				map[string]any{"type": "drop column", "name": "***"},
				map[string]any{"type": "rename column"},
				map[string]any{"type": "Rename column", "name": 32, "to": 3.2},
				map[string]any{"type": "rename COLUMN", "name": "***", "to": "!nope"},
				map[string]any{"type": "add column"},
				map[string]any{"type": "add column", "column": true},
				map[string]any{"type": "Add Column", "column": map[string]any{"name": "*nope", "type": "wrong", "nullable": 1.4, "default": []any{}}},
			},
		}).
		Put(Update).
		ExpectValidation(
			"changes.0.to", 1001,
			"changes.1.to", 1002,
			"changes.2.to", 1004,
			"changes.3.name", 1001,
			"changes.4.name", 1002,
			"changes.5.name", 1004,
			"changes.6.name", 1001, "changes.6.to", 1001,
			"changes.7.name", 1002, "changes.7.to", 1002,
			"changes.8.name", 1004, "changes.8.to", 1004,
			"changes.9.column", 1001,
			"changes.10.column", 1022,
			"changes.11.column.name", 1004, "changes.11.column.type", 1015, "changes.11.column.nullable", 1009,
		)
}

func Test_Update_UnknownTable(t *testing.T) {
	request.ReqT(t, sqlkite.BuildEnv().Env()).
		UserValue("name", "test1").
		Put(Update).
		ExpectValidation("", 302_033)
}

// We could catch this specific error and provide structured error, so this test
// could need to be re-thought. It'd probably easier to test this behavior direct
// against project.Update, since that expects a clean/validated input. We could.
// for example, rename the table to an invalid name there.
func Test_Update_SQL_Error(t *testing.T) {
	id := tests.Factory.DynamicId()
	project, _ := sqlkite.Projects.Get(id)
	request.ReqT(t, project.Env()).
		Body(map[string]any{
			"name": "Test_Update_SQL_Error",
			"columns": []any{
				map[string]any{"name": "c1", "type": "text", "nullable": true, "default": "a"},
			},
		}).
		Post(Create).
		OK()

	var errorId string
	log := tests.CaptureLog(func() {
		project, _ = sqlkite.Projects.Get(id)
		res := request.ReqT(t, project.Env()).
			Body(map[string]any{
				"changes": []any{
					map[string]any{"type": "add column", "column": map[string]any{"name": "C1", "type": "text", "nullable": true}},
				},
			}).
			UserValue("name", "Test_Update_SQL_Error").
			Put(Update).
			ExpectStatus(500)
		errorId = res.Json.String("error_id")
	})

	assert.StringContains(t, log, "duplicate column name: c1")
	assert.StringContains(t, log, "eid="+errorId)
}

func Test_Update_Success(t *testing.T) {
	id := tests.Factory.DynamicId()
	project, _ := sqlkite.Projects.Get(id)
	request.ReqT(t, project.Env()).
		Body(map[string]any{
			"name": "Test_Update_Success",
			"columns": []any{
				map[string]any{"name": "c1", "type": "text", "nullable": true},
				map[string]any{"name": "c2", "type": "int", "nullable": true},
			},
			"access": map[string]any{"select": "select 1"},
		}).
		Post(Create).
		OK()

	// keep existing access control
	project, _ = sqlkite.Projects.Get(id)
	request.ReqT(t, project.Env()).
		Body(map[string]any{
			"changes": []any{
				map[string]any{"type": "rename COLUMN", "name": "c1", "to": "c1_b"},
				map[string]any{"type": "DROP COLUMN", "name": "c2"},
				map[string]any{"type": "add column", "column": map[string]any{"name": "C3", "type": "real", "nullable": true, "default": 3.19}},
				map[string]any{"type": "rename", "to": "Test_Update_Success_b"},
			},
		}).
		UserValue("name", "Test_Update_Success").
		Put(Update).
		OK()

	project, _ = sqlkite.Projects.Get(id)
	table, _ := project.Table("test_update_success_b")
	columns := table.Columns
	assert.Equal(t, table.Access.Select.CTE, "select 1")
	assert.Equal(t, table.Access.Select.Name, "sqlkite_cte_test_update_success_b")
	assert.Equal(t, len(columns), 2)
	assert.Equal(t, columns[0].Name, "c1_b")
	assert.Equal(t, columns[0].Type, data.COLUMN_TYPE_TEXT)
	assert.Equal(t, columns[1].Name, "c3")
	assert.Equal(t, columns[1].Type, data.COLUMN_TYPE_REAL)
	assert.Equal(t, columns[1].Nullable, true)
	assert.Equal(t, columns[1].Default.(float64), 3.19)

	// alter access control
	request.ReqT(t, project.Env()).
		Body(map[string]any{
			"access": map[string]any{"select": "select 2"},
		}).
		UserValue("name", "Test_Update_Success_b").
		Put(Update).
		OK()

	project, _ = sqlkite.Projects.Get(id)
	table, _ = project.Table("test_update_success_b")
	assert.Equal(t, table.Access.Select.CTE, "select 2")
	assert.Equal(t, table.Access.Select.Name, "sqlkite_cte_test_update_success_b")
	assert.Equal(t, len(table.Columns), 2)

	// remove access control
	request.ReqT(t, project.Env()).
		Body(map[string]any{
			"access": map[string]any{"select": ""},
		}).
		UserValue("name", "Test_Update_Success_b").
		Put(Update).
		OK()

	project, _ = sqlkite.Projects.Get(id)
	table, _ = project.Table("test_update_success_b")
	assert.Nil(t, table.Access.Select)
	assert.Equal(t, len(table.Columns), 2)
}
