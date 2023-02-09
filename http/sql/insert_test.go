package sql

import (
	"testing"

	"src.goblgobl.com/sqlite"
	"src.goblgobl.com/tests/assert"
	"src.goblgobl.com/tests/request"
	"src.goblgobl.com/utils"
	"src.goblgobl.com/utils/typed"
	"src.sqlkite.com/sqlkite"
	"src.sqlkite.com/sqlkite/codes"
	"src.sqlkite.com/sqlkite/tests"
)

func Test_Insert_InvalidBody(t *testing.T) {
	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body("nope").
		Post(Insert).
		ExpectInvalid(utils.RES_INVALID_JSON_PAYLOAD)
}

func Test_Insert_InvalidData(t *testing.T) {
	// required fields
	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body("{}").
		Post(Insert).
		ExpectValidation("into", utils.VAL_REQUIRED, "columns", utils.VAL_REQUIRED).ExpectNoValidation("returning")

	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body(map[string]any{
			"into":      1,
			"columns":   true,
			"returning": 3.18,
		}).
		Post(Insert).
		ExpectValidation("into", codes.VAL_INVALID_TABLE_NAME, "columns", utils.VAL_ARRAY_TYPE, "returning", utils.VAL_ARRAY_TYPE)

	request.ReqT(t, standardProject.Env()).
		Body(map[string]any{
			"into":      "",
			"columns":   []any{"", "$"},
			"returning": []any{"ok", "$"},
		}).
		Post(Insert).
		ExpectValidation("into", codes.VAL_INVALID_TABLE_NAME, "columns.0", codes.VAL_INVALID_COLUMN_NAME, "columns.1", codes.VAL_INVALID_COLUMN_NAME, "returning.1", codes.VAL_INVALID_COLUMN_NAME)

	// We don't fully test the parser. The parser has tests for that. We just
	// want to test that we handle parser errors correctly.
	request.ReqT(t, standardProject.Env()).
		Body(map[string]any{
			"into": "$nope",
		}).
		Post(Insert).
		ExpectValidation("into", codes.VAL_INVALID_TABLE_NAME)
}

func Test_Insert_InvalidTable(t *testing.T) {
	request.ReqT(t, standardProject.Env()).
		Body(map[string]any{
			"columns": []string{"id"},
			"into":    "not_a_real_table",
		}).
		Post(Insert).
		ExpectValidation("into", codes.VAL_UNKNOWN_TABLE)
}

func Test_Insert_OverLimits(t *testing.T) {
	request.ReqT(t, limitedProject.Env()).
		Body(map[string]any{
			"into":       "tab1",
			"columns":    []any{"a"},
			"parameters": []any{1, 2, 3},
		}).
		Post(Insert).
		ExpectValidation("parameters", codes.VAL_SQL_TOO_MANY_PARAMETERS)
}

func Test_Insert_AtLimits(t *testing.T) {
	res := request.ReqT(t, limitedProject.Env()).
		Body(map[string]any{
			"into":       "t1",
			"columns":    []any{"id", "name"},
			"parameters": []any{1, "leto"},
		}).
		Post(Insert).
		OK()

	assert.Equal(t, res.Body, `{"affected":1}`)
}

func Test_Insert_A_SingleRow(t *testing.T) {
	id := tests.Factory.DynamicId()
	project, _ := sqlkite.Projects.Get(id)

	res := request.ReqT(t, project.Env()).
		Body(map[string]any{
			"into":       "products",
			"columns":    []any{"id", "name", "rating"},
			"parameters": []any{2, "tea", 9.9},
		}).
		Post(Insert).
		OK()

	assert.Equal(t, res.Body, `{"affected":1}`)

	row := getRow(project, "select * from products where id = ?1", 2)
	assert.Equal(t, row.String("name"), "tea")
	assert.Equal(t, row.Float("rating"), 9.9)
}

func Test_Insert_A_SingleRow_Returning(t *testing.T) {
	id := tests.Factory.DynamicId()
	project, _ := sqlkite.Projects.Get(id)

	res := request.ReqT(t, project.Env()).
		Body(map[string]any{
			"into":       "products",
			"columns":    []any{"id", "name", "rating"},
			"parameters": []any{4, "tea", 9.9},
			"returning":  []any{"id", "name"},
		}).
		Post(Insert).
		OK()

	assert.Equal(t, res.Body, `{"r":[{"id":4,"name":"tea"}]}`)
}

func Test_Insert_A_MultipleRows(t *testing.T) {
	id := tests.Factory.DynamicId()
	project, _ := sqlkite.Projects.Get(id)

	res := request.ReqT(t, project.Env()).
		Body(map[string]any{
			"into":       "products",
			"columns":    []any{"id", "name", "rating"},
			"parameters": []any{2, "tea", 9.9, 3, "chocholate", 9.2},
		}).
		Post(Insert).
		OK()

	assert.Equal(t, res.Body, `{"affected":2}`)

	rows := getRows(project, "select * from products where id in (2, 3) order by id")
	assert.Equal(t, rows[0].Int("id"), 2)
	assert.Equal(t, rows[0].String("name"), "tea")
	assert.Equal(t, rows[0].Float("rating"), 9.9)

	assert.Equal(t, rows[1].Int("id"), 3)
	assert.Equal(t, rows[1].String("name"), "chocholate")
	assert.Equal(t, rows[1].Float("rating"), 9.2)
}

func Test_Insert_A_MultipleRows_Returning(t *testing.T) {
	id := tests.Factory.DynamicId()
	project, _ := sqlkite.Projects.Get(id)

	res := request.ReqT(t, project.Env()).
		Body(map[string]any{
			"into":       "products",
			"columns":    []any{"id", "name", "rating"},
			"parameters": []any{2, "tea", 9.9, 3, "chocholate", 9.2},
			"returning":  []any{"id", "rating"},
		}).
		Post(Insert).
		OK()

	assert.Equal(t, res.Body, `{"r":[{"id":2,"rating":9.9},{"id":3,"rating":9.2}]}`)
}

func getRow(project *sqlkite.Project, sql string, args ...any) typed.Typed {
	var t typed.Typed
	project.WithDB(func(conn sqlite.Conn) {
		m, err := conn.RowArr(sql, args).Map()
		if err != nil {
			if err == sqlite.ErrNoRows {
				return
			}
			panic(err)
		}
		t = typed.Typed(m)
	})
	return t
}

func getRows(project *sqlkite.Project, sql string, args ...any) []typed.Typed {
	t := make([]typed.Typed, 0, 10)
	project.WithDB(func(conn sqlite.Conn) {
		rows := conn.RowsArr(sql, args)
		defer rows.Close()

		for rows.Next() {
			m, err := rows.Stmt.Map()
			if err != nil {
				panic(err)
			}
			t = append(t, typed.Typed(m))
		}
		if err := rows.Error(); err != nil {
			panic(err)
		}
	})
	return t
}
