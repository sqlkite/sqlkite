package sql

import (
	"testing"

	"src.goblgobl.com/tests/assert"
	"src.goblgobl.com/tests/request"
	"src.goblgobl.com/utils"
	"src.sqlkite.com/sqlkite"
	"src.sqlkite.com/sqlkite/codes"
	"src.sqlkite.com/sqlkite/tests"
)

var (
	limitedProject  *sqlkite.Project
	standardProject *sqlkite.Project
)

func init() {
	limitedProject, _ = sqlkite.Projects.Get(tests.Factory.LimitedId)
	standardProject, _ = sqlkite.Projects.Get(tests.Factory.StandardId)
}

func Test_Select_InvalidBody(t *testing.T) {
	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body("nope").
		Post(Select).
		ExpectInvalid(utils.RES_INVALID_JSON_PAYLOAD)
}

func Test_Select_InvalidData(t *testing.T) {
	// required fields
	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body("{}").
		Post(Select).
		ExpectValidation("select", utils.VAL_REQUIRED, "from", utils.VAL_REQUIRED)

	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body(map[string]any{
			"select":     1,
			"from":       2,
			"where":      3,
			"parameters": 4,
			"limit":      "a",
			"offset":     []int{},
		}).
		Post(Select).
		ExpectValidation("select", utils.VAL_ARRAY_TYPE, "from", utils.VAL_ARRAY_TYPE, "where", utils.VAL_ARRAY_TYPE, "parameters", utils.VAL_ARRAY_TYPE, "limit", utils.VAL_INT_TYPE, "offset", utils.VAL_INT_TYPE)

	// We don't fully test the parser. The parser has tests for that. We just
	// want to test that we handle parser errors correctly.
	request.ReqT(t, standardProject.Env()).
		Body(map[string]any{
			"select": []string{"$nope"},
			"from":   []any{"table1", []string{"invalidjoin", "table", "alias"}},
			"where":  []any{[]string{"id", "====", "?1"}},
		}).
		Post(Select).
		ExpectValidation("select.0", codes.VAL_INVALID_COLUMN_NAME, "from.1", codes.VAL_INVALID_JOINABLE_FROM_JOIN, "where", 301_005)
}

func Test_Select_InvalidTable(t *testing.T) {
	request.ReqT(t, standardProject.Env()).
		Body(map[string]any{
			"select": []string{"id"},
			"from":   []string{"not_a_real_table"},
		}).
		Post(Select).
		ExpectValidation("", codes.VAL_UNKNOWN_TABLE)
}

func Test_Select_InvalidColumn(t *testing.T) {
	var errorId string
	log := tests.CaptureLog(func() {
		res := request.ReqT(t, standardProject.Env()).
			Body(map[string]any{
				"select": []string{"not_a_column"},
				"from":   []string{"products"},
			}).
			Post(Select).
			ExpectStatus(500)

		errorId = res.Json.String("error_id")
	})

	assert.StringContains(t, log, "no such column: not_a_column (code: 1)")
	assert.StringContains(t, log, "eid="+errorId)
}

func Test_Select_SQLTooLong(t *testing.T) {
	request.ReqT(t, limitedProject.Env()).
		Body(map[string]any{
			"select": []string{"this_id_is_quite_long", "and_another_really_long_one"},
			"from":   []string{"t1"},
		}).
		Post(Select).
		ExpectValidation("", codes.VAL_SQL_TOO_LONG)
}

func Test_Select_OverLimits(t *testing.T) {
	request.ReqT(t, limitedProject.Env()).
		Body(map[string]any{
			"select":     []string{"id", "id", "id"},
			"from":       []string{"t1", "t2", "t3"},
			"limit":      3,
			"order":      []any{"id", "id", "id"},
			"parameters": []any{1, 2, 3},
		}).
		Post(Select).
		ExpectValidation(
			"parameters", codes.VAL_SQL_TOO_MANY_PARAMETERS,
			"from", codes.VAL_TOO_MANY_FROMS,
			"limit", codes.VAL_SQL_LIMIT_TOO_HIGH,
			"select", codes.VAL_SQL_TOO_MANY_SELECT,
			"order", codes.VAL_SQL_TOO_MANY_ORDER_BY)
}

func Test_Select_AtLimits(t *testing.T) {
	res := request.ReqT(t, limitedProject.Env()).
		Body(map[string]any{
			"select":     []string{"?1"},
			"from":       []string{"t1", "t2"},
			"limit":      2,
			"order":      []any{"id", "-id"},
			"parameters": []any{1, 0},
			"where":      []any{[]string{"id", "=", "?2"}},
		}).
		Post(Select).
		OK()

	assert.Equal(t, res.Body, `{"r":[]}`)
}

func Test_Select_Empty_Resut(t *testing.T) {
	res := request.ReqT(t, standardProject.Env()).
		Body(map[string]any{
			"select":     []string{"id"},
			"from":       []string{"products"},
			"where":      []any{[]string{"id", "=", "?1"}},
			"parameters": []any{0},
		}).
		Post(Select).
		OK()

	assert.Equal(t, res.Body, `{"r":[]}`)
}

func Test_Select_Single_Row_Single_Column(t *testing.T) {
	p, _ := sqlkite.Projects.Get(tests.Factory.StandardId)
	res := request.ReqT(t, p.Env()).
		Body(map[string]any{
			"select":     []string{"id"},
			"from":       []string{"products"},
			"where":      []any{[]string{"id", "=", "?1"}},
			"parameters": []any{1},
		}).
		Post(Select).
		OK()

	assert.Equal(t, res.Body, `{"r":[{"id":1}]}`)
}

func Test_Select_Single_Row_Multi_Column(t *testing.T) {
	p, _ := sqlkite.Projects.Get(tests.Factory.StandardId)
	res := request.ReqT(t, p.Env()).
		Body(map[string]any{
			"select":     []string{"id", "name", "rating", "image"},
			"from":       []string{"products"},
			"where":      []any{[]string{"id", "=", "?1"}},
			"parameters": []any{1},
		}).
		Post(Select).
		OK()

	assert.Equal(t, res.Body, `{"r":[{"id":1,"name":"KF99","rating":4.8,"image":null}]}`)
}

func Test_Select_Paging(t *testing.T) {
	p, _ := sqlkite.Projects.Get(tests.Factory.StandardId)
	res := request.ReqT(t, p.Env()).
		Body(map[string]any{
			"select":     []string{"id", "name"},
			"from":       []string{"products"},
			"where":      []any{[]string{"id", "<=", "?1"}},
			"order":      []any{"-id"},
			"limit":      2,
			"offset":     0,
			"parameters": []any{3},
		}).
		Post(Select).
		OK()

	assert.Equal(t, res.Body, `{"r":[{"id":3,"name":"Keemun"},{"id":2,"name":"Absolute"}]}`)

	//next page
	res = request.ReqT(t, p.Env()).
		Body(map[string]any{
			"select":     []string{"id", "name"},
			"from":       []string{"products"},
			"where":      []any{[]string{"id", "<=", "?1"}},
			"order":      []any{"-id"},
			"limit":      2,
			"offset":     2,
			"parameters": []any{3},
		}).
		Post(Select).
		OK()
	assert.Equal(t, res.Body, `{"r":[{"id":1,"name":"KF99"}]}`)

	//no more results at this page
	res = request.ReqT(t, p.Env()).
		Body(map[string]any{
			"select":     []string{"id", "name"},
			"from":       []string{"products"},
			"where":      []any{[]string{"id", "<=", "?1"}},
			"order":      []any{"-id"},
			"limit":      2,
			"offset":     3,
			"parameters": []any{3},
		}).
		Post(Select).
		OK()
	assert.Equal(t, res.Body, `{"r":[]}`)
}

func Test_Select_AccessControl_NoUser(t *testing.T) {
	p, _ := sqlkite.Projects.Get(tests.Factory.StandardId)
	res := request.ReqT(t, p.Env()).
		Body(map[string]any{
			"select": []string{"id", "public"},
			"from":   []string{"users"},
		}).
		Post(Select).
		OK()
	assert.Equal(t, res.Body, `{"r":[{"id":3,"public":1},{"id":4,"public":1}]}`)
}
