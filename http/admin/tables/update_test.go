package tables

import (
	"testing"

	"src.goblgobl.com/sqlite"
	"src.goblgobl.com/tests/assert"
	"src.goblgobl.com/tests/request"
	"src.sqlkite.com/sqlkite"
	"src.sqlkite.com/sqlkite/data"
	"src.sqlkite.com/sqlkite/tests"
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
			"access": 9,
		}).
		Put(Update).
		ExpectValidation("changes.0", 1022, "changes.1.type", 1001, "changes.2.type", 1015, "access", 1022)

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
			"access": map[string]any{"select": 32, "insert": "no", "update": true, "delete": []any{}},
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
			"access.select", 1002, "access.insert", 1022, "access.update", 1022, "access.delete", 1022,
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
			"name": "Test_Update_Success",
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
	assert.Equal(t, len(columns), 2)
	assert.Equal(t, columns[0].Name, "c1_b")
	assert.Equal(t, columns[0].Type, data.COLUMN_TYPE_TEXT)
	assert.Equal(t, columns[1].Name, "c3")
	assert.Equal(t, columns[1].Type, data.COLUMN_TYPE_REAL)
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
	insertAccessControl := tests.SqliteMaster(project, "sqlkite_row_access_test_update_success_b_insert", "test_update_success_b")
	assert.StringContains(t, insertAccessControl, `select 10`)
	updateAccessControl := tests.SqliteMaster(project, "sqlkite_row_access_test_update_success_b_update", "test_update_success_b")
	assert.StringContains(t, updateAccessControl, `select 11`)
	deleteAccessControl := tests.SqliteMaster(project, "sqlkite_row_access_test_update_success_b_delete", "test_update_success_b")
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
		UserValue("name", "Test_Update_Success_b").
		Put(Update).
		OK()

	project, _ = sqlkite.Projects.Get(id)
	table, _ = project.Table("test_update_success_b")
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
	insertAccessControl = tests.SqliteMaster(project, "sqlkite_row_access_test_update_success_b_insert", "test_update_success_b")
	assert.StringContains(t, insertAccessControl, `select 20`)
	updateAccessControl = tests.SqliteMaster(project, "sqlkite_row_access_test_update_success_b_update", "test_update_success_b")
	assert.StringContains(t, updateAccessControl, `select 21`)
	deleteAccessControl = tests.SqliteMaster(project, "sqlkite_row_access_test_update_success_b_delete", "test_update_success_b")
	assert.StringContains(t, deleteAccessControl, `select 22`)

	// remove access control
	request.ReqT(t, project.Env()).
		Body(map[string]any{
			"access": map[string]any{
				"select": "",
				"insert": map[string]any{"trigger": ""},
				"update": map[string]any{"trigger": ""},
				"delete": map[string]any{"trigger": ""},
			},
		}).
		UserValue("name", "Test_Update_Success_b").
		Put(Update).
		OK()

	project, _ = sqlkite.Projects.Get(id)
	table, _ = project.Table("test_update_success_b")
	assert.Nil(t, table.Access.Select)

	assertTriggerCount(project, "test_update_success", 0)
	assertTriggerCount(project, "test_update_success_b", 0)
	assert.Nil(t, table.Access.Insert)
	assert.Nil(t, table.Access.Update)
	assert.Nil(t, table.Access.Delete)
	assert.Equal(t, len(table.Columns), 2)
}
