package tables

import (
	"testing"

	"src.goblgobl.com/sqlite"
	"src.goblgobl.com/tests/assert"
	"src.goblgobl.com/tests/request"
	"src.goblgobl.com/utils"
	"src.sqlkite.com/sqlkite"
	"src.sqlkite.com/sqlkite/codes"
	"src.sqlkite.com/sqlkite/tests"
)

func Test_Update_InvalidBody(t *testing.T) {
	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body("nope").
		Put(Update).
		ExpectInvalid(utils.RES_INVALID_JSON_PAYLOAD)
}

func Test_Update_InvalidData(t *testing.T) {
	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body(map[string]any{
			"changes": "4",
		}).
		Put(Update).
		ExpectValidation("changes", utils.VAL_ARRAY_TYPE).ExpectNoValidation("max_update_count", "max_delete_count")

	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body(map[string]any{
			"changes": []any{
				4,
				map[string]any{},
				map[string]any{"type": "nope"},
			},
			"access":           9,
			"max_update_count": "two",
			"max_delete_count": "three",
		}).
		Put(Update).
		ExpectValidation("changes.0", utils.VAL_OBJECT_TYPE, "changes.1.type", utils.VAL_REQUIRED, "changes.2.type", utils.VAL_STRING_CHOICE, "access", utils.VAL_OBJECT_TYPE, "max_update_count", utils.VAL_INT_TYPE, "max_delete_count", utils.VAL_INT_TYPE)

	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body(map[string]any{
			"max_update_count": -4,
			"max_delete_count": -5,
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
			"access": map[string]any{"select": 32, "insert": "no", "update": true, "delete": []any{}},
		}).
		Put(Update).
		Inspect().
		ExpectValidation(
			"max_update_count", utils.VAL_INT_MIN,
			"max_delete_count", utils.VAL_INT_MIN,
			"changes.0.to", utils.VAL_REQUIRED,
			"changes.1.to", utils.VAL_STRING_TYPE,
			"changes.2.to", utils.VAL_STRING_PATTERN,
			"changes.3.name", utils.VAL_REQUIRED,
			"changes.4.name", utils.VAL_STRING_TYPE,
			"changes.5.name", utils.VAL_STRING_PATTERN,
			"changes.6.name", utils.VAL_REQUIRED, "changes.6.to", utils.VAL_REQUIRED,
			"changes.7.name", utils.VAL_STRING_TYPE, "changes.7.to", utils.VAL_STRING_TYPE,
			"changes.8.name", utils.VAL_STRING_PATTERN, "changes.8.to", utils.VAL_STRING_PATTERN,
			"changes.9.column", utils.VAL_REQUIRED,
			"changes.10.column", utils.VAL_OBJECT_TYPE,
			"changes.11.column.name", utils.VAL_STRING_PATTERN, "changes.11.column.type", utils.VAL_STRING_CHOICE, "changes.11.column.nullable", utils.VAL_BOOL_TYPE,
			"access.select", utils.VAL_STRING_TYPE, "access.insert", utils.VAL_OBJECT_TYPE, "access.update", utils.VAL_OBJECT_TYPE, "access.delete", utils.VAL_OBJECT_TYPE,
		)
}

func Test_Update_UnknownTable(t *testing.T) {
	request.ReqT(t, sqlkite.BuildEnv().Env()).
		UserValue("name", "test1").
		Put(Update).
		ExpectValidation("", codes.VAL_UNKNOWN_TABLE)
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
					map[string]any{"type": "add column", "column": map[string]any{"name": "c1", "type": "text", "nullable": true}},
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
	assertTriggerCount := func(project *sqlkite.Project, tableName string, expectedCount int) {
		project.WithDB(func(conn sqlite.Conn) {
			var actual int
			sql := "select count(*) from sqlite_master where tbl_name = ?1 and type = 'trigger'"
			if err := conn.Row(sql, tableName).Scan(&actual); err != nil {
				panic(err)
			}
			assert.Equal(t, actual, expectedCount)
		})
	}

	id := tests.Factory.DynamicId()
	project, _ := sqlkite.Projects.Get(id)
	request.ReqT(t, project.Env()).
		Body(map[string]any{
			"name": "test_update_success",
			"columns": []any{
				map[string]any{"name": "c1", "type": "text", "nullable": true},
				map[string]any{"name": "c2", "type": "int", "nullable": true},
			},
			"access": map[string]any{
				"select": "select 1",
				"insert": map[string]any{"when": "10=10", "trigger": "select 10"},
				"update": map[string]any{"when": "11=11", "trigger": "select 11"},
				"delete": map[string]any{"when": "12=12", "trigger": "select 12"},
			},
		}).
		Post(Create).
		OK()

	// keep existing access control
	project, _ = sqlkite.Projects.Get(id)
	request.ReqT(t, project.Env()).
		Body(map[string]any{
			"max_update_count": 5,
			"max_delete_count": 6,
			"changes": []any{
				map[string]any{"type": "rename COLUMN", "name": "c1", "to": "c1_b"},
				map[string]any{"type": "DROP COLUMN", "name": "c2"},
				map[string]any{"type": "add column", "column": map[string]any{"name": "C3", "type": "real", "nullable": true, "default": 3.19}},
				map[string]any{"type": "rename", "to": "test_update_success_b"},
			},
		}).
		UserValue("name", "test_update_success").
		Put(Update).
		OK()

	project, _ = sqlkite.Projects.Get(id)
	table := project.Table("test_update_success_b")
	assert.Equal(t, table.MaxUpdateCount.Value, 5)
	assert.Equal(t, table.MaxDeleteCount.Value, 6)

	columns := table.Columns
	assert.Equal(t, len(columns), 2)
	assert.Equal(t, columns[0].Name, "c1_b")
	assert.Equal(t, columns[0].Type, sqlkite.COLUMN_TYPE_TEXT)
	assert.Equal(t, columns[1].Name, "C3")
	assert.Equal(t, columns[1].Type, sqlkite.COLUMN_TYPE_REAL)
	assert.Equal(t, columns[1].Nullable, true)
	assert.Equal(t, columns[1].Default.(float64), 3.19)

	assert.Equal(t, table.Access.Select.CTE, "select 1")
	assert.Equal(t, table.Access.Select.Name, "sqlkite_cte_test_update_success_b")

	assertTriggerCount(project, "test_update_success", 0)
	assertTriggerCount(project, "test_update_success_b", 3)
	assert.Equal(t, table.Access.Insert.When, "10=10")
	assert.Equal(t, table.Access.Insert.Trigger, "select 10")
	assert.Equal(t, table.Access.Update.When, "11=11")
	assert.Equal(t, table.Access.Update.Trigger, "select 11")
	assert.Equal(t, table.Access.Delete.When, "12=12")
	assert.Equal(t, table.Access.Delete.Trigger, "select 12")
	insertAccessControl := tests.SqliteMaster(project, "sqlkite_ra_test_update_success_b_i", "test_update_success_b")
	assert.StringContains(t, insertAccessControl, `select 10`)
	updateAccessControl := tests.SqliteMaster(project, "sqlkite_ra_test_update_success_b_u", "test_update_success_b")
	assert.StringContains(t, updateAccessControl, `select 11`)
	deleteAccessControl := tests.SqliteMaster(project, "sqlkite_ra_test_update_success_b_d", "test_update_success_b")
	assert.StringContains(t, deleteAccessControl, `select 12`)

	// alter access control
	request.ReqT(t, project.Env()).
		Body(map[string]any{
			"access": map[string]any{
				"select": "select 2",
				"insert": map[string]any{"when": "20=20", "trigger": "select 20"},
				"update": map[string]any{"when": "21=21", "trigger": "select 21"},
				"delete": map[string]any{"when": "22=22", "trigger": "select 22"},
			},
		}).
		UserValue("name", "test_update_success_b").
		Put(Update).
		OK()

	project, _ = sqlkite.Projects.Get(id)
	table = project.Table("test_update_success_b")
	assert.False(t, table.MaxUpdateCount.Exists)
	assert.False(t, table.MaxDeleteCount.Exists)

	assert.Equal(t, len(table.Columns), 2)

	assert.Equal(t, table.Access.Select.CTE, "select 2")
	assert.Equal(t, table.Access.Select.Name, "sqlkite_cte_test_update_success_b")

	assertTriggerCount(project, "test_update_success", 0)
	assertTriggerCount(project, "test_update_success_b", 3)
	assert.Equal(t, table.Access.Insert.When, "20=20")
	assert.Equal(t, table.Access.Insert.Trigger, "select 20")
	assert.Equal(t, table.Access.Update.When, "21=21")
	assert.Equal(t, table.Access.Update.Trigger, "select 21")
	assert.Equal(t, table.Access.Delete.When, "22=22")
	assert.Equal(t, table.Access.Delete.Trigger, "select 22")
	insertAccessControl = tests.SqliteMaster(project, "sqlkite_ra_test_update_success_b_i", "test_update_success_b")
	assert.StringContains(t, insertAccessControl, `select 20`)
	updateAccessControl = tests.SqliteMaster(project, "sqlkite_ra_test_update_success_b_u", "test_update_success_b")
	assert.StringContains(t, updateAccessControl, `select 21`)
	deleteAccessControl = tests.SqliteMaster(project, "sqlkite_ra_test_update_success_b_d", "test_update_success_b")
	assert.StringContains(t, deleteAccessControl, `select 22`)

	// remove access control
	request.ReqT(t, project.Env()).
		Body(map[string]any{
			"access": map[string]any{
				"select": nil,
				"insert": nil,
				"update": nil,
				"delete": nil,
			},
		}).
		UserValue("name", "test_update_success_b").
		Put(Update).
		OK()

	// make sure the project isn't mutated
	table = project.Table("test_update_success_b")
	assert.NotNil(t, table.Access.Select)

	// but if we reload the project, now we get the latest changes
	project, _ = sqlkite.Projects.Get(id)
	table = project.Table("test_update_success_b")
	assert.Nil(t, table.Access.Select)

	assertTriggerCount(project, "test_update_success", 0)
	assertTriggerCount(project, "test_update_success_b", 0)
	assert.Nil(t, table.Access.Insert)
	assert.Nil(t, table.Access.Update)
	assert.Nil(t, table.Access.Delete)
	assert.Equal(t, len(table.Columns), 2)
}
