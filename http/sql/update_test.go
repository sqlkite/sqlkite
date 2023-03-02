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

func Test_Update_InvalidBody(t *testing.T) {
	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body("nope").
		Post(Update).
		ExpectInvalid(utils.RES_INVALID_JSON_PAYLOAD)
}

func Test_Update_InvalidData(t *testing.T) {
	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body("{}").
		Post(Update).
		ExpectValidation("target", utils.VAL_REQUIRED)

	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body(map[string]any{
			"target": 1,
		}).
		Post(Update).
		ExpectValidation("target", codes.VAL_INVALID_TABLE_NAME)

	request.ReqT(t, standardProject.Env()).
		Body(map[string]any{
			"target":     "products",
			"set":        true,
			"returning":  3.18,
			"where":      3,
			"from":       2,
			"parameters": 4,
			"offset":     []int{},
		}).
		Post(Update).
		ExpectValidation("set", utils.VAL_OBJECT_TYPE, "returning", utils.VAL_ARRAY_TYPE, "from", utils.VAL_ARRAY_TYPE, "where", utils.VAL_ARRAY_TYPE, "parameters", utils.VAL_ARRAY_TYPE, "offset", utils.VAL_INT_TYPE)

	request.ReqT(t, standardProject.Env()).
		Body(map[string]any{
			"target":    "products",
			"set":       map[string]any{"$hi": "$", "valid": 32},
			"returning": []any{"ok", "$"},
		}).
		Post(Update).
		// Inspect().
		ExpectValidation("set", codes.VAL_INVALID_COLUMN_NAME, "returning.1", codes.VAL_INVALID_COLUMN_NAME).
		ExpectNoValidation("returning", "from", "limit", "offset", "order", "where")

	// We don't fully test the parser. The parser has tests for that. We just
	// want to test that we handle parser errors correctly.
	request.ReqT(t, standardProject.Env()).
		Body(map[string]any{
			"target": "$nope",
		}).
		Post(Update).
		ExpectValidation("target", codes.VAL_INVALID_TABLE_NAME)
}

func Test_Update_InvalidTable(t *testing.T) {
	request.ReqT(t, standardProject.Env()).
		Body(map[string]any{
			"set":    map[string]any{"id": "?1"},
			"target": "not_a_real_table",
		}).
		Post(Update).
		ExpectValidation("target", codes.VAL_UNKNOWN_TABLE)
}

func Test_Update_OverLimits(t *testing.T) {
	request.ReqT(t, limitedProject.Env()).
		Body(map[string]any{
			"target":     "tab1",
			"set":        map[string]any{"id": "?1"},
			"parameters": []any{1, 2, 3},
		}).
		Post(Update).
		ExpectValidation("parameters", codes.VAL_SQL_TOO_MANY_PARAMETERS)
}

func Test_Update_Nothing(t *testing.T) {
	res := request.ReqT(t, standardProject.Env()).
		Body(map[string]any{
			"target":     "products",
			"set":        map[string]any{"name": "?1", "rating": "?2"},
			"where":      []any{[]string{"?2", "=", "?3"}},
			"parameters": []any{"leto", 1, 0},
		}).
		Post(Update).
		OK()

	assert.Equal(t, res.Body, `{"affected":0}`)
}

func Test_Update_AtLimits(t *testing.T) {
	res := request.ReqT(t, limitedProject.Env()).
		Body(map[string]any{
			"target":     "t1",
			"set":        map[string]any{"name": "?1"},
			"where":      []any{[]string{"id", "=", "?2"}},
			"parameters": []any{"leto", -999999},
		}).
		Post(Update).
		OK()

	assert.Equal(t, res.Body, `{"affected":0}`)
}

func Test_Update_Placeholder_Invalid_Index(t *testing.T) {
	request.ReqT(t, standardProject.Env()).
		Body(map[string]any{
			"target":     "products",
			"set":        map[string]any{"name": "?3"},
			"parameters": []any{"leto"},
		}).
		Post(Update).
		ExpectValidation("row.name", codes.VAL_PLACEHOLDER_INDEX_OUT_OF_RANGE)
}

func Test_Update_Column_Deny(t *testing.T) {
	id := tests.Factory.DynamicId()
	project, _ := sqlkite.Projects.Get(id)

	request.ReqT(t, project.Env()).
		Body(map[string]any{
			"target":     "products",
			"set":        map[string]any{"id": "?1"},
			"parameters": []any{55},
		}).
		Post(Update).
		ExpectValidation("set", codes.VAL_COLUMN_UPDATE_DENY)
}

func Test_Update_Validates_Parameters(t *testing.T) {
	id := tests.Factory.DynamicId()
	project, _ := sqlkite.Projects.Get(id)

	request.ReqT(t, project.Env()).
		Body(map[string]any{
			"target":     "products",
			"set":        map[string]any{"name": "?1"},
			"parameters": []any{55},
		}).
		Post(Update).
		ExpectValidation("row.name", utils.VAL_STRING_TYPE)

	request.ReqT(t, project.Env()).
		Body(map[string]any{
			"target":     "products",
			"set":        map[string]any{"name": "?1"},
			"parameters": []any{"a"},
		}).
		Post(Update).
		ExpectValidation("row.name", utils.VAL_STRING_LENGTH)
}

func Test_Update_SingleRow(t *testing.T) {
	id := tests.Factory.DynamicId()
	project, _ := sqlkite.Projects.Get(id)

	tests.Factory.Product.Insert(project, "id", 9999, "name", "tea", "rating", 9.9)

	res := request.ReqT(t, project.Env()).
		Body(map[string]any{
			"target":     "products",
			"set":        map[string]any{"name": "?1", "rating": "?2"},
			"where":      []any{[]string{"id", "=", "?3"}},
			"parameters": []any{"tea2", 8.9, 9999},
		}).
		Post(Update).
		OK()

	assert.Equal(t, res.Body, `{"affected":1}`)

	row := getRow(project, "select * from products where id = ?1", 9999)
	assert.Equal(t, row.String("name"), "tea2")
	assert.Equal(t, row.Float("rating"), 8.9)
}

func Test_Update_LimitOffsetReturningOrder(t *testing.T) {
	id := tests.Factory.DynamicId()
	project, _ := sqlkite.Projects.Get(id)

	tests.Factory.Product.Insert(project, "id", 1, "name", "p1", "rating", 1.1)
	tests.Factory.Product.Insert(project, "id", 2, "name", "p2", "rating", 2.2)
	tests.Factory.Product.Insert(project, "id", 3, "name", "p3", "rating", 3.3)
	tests.Factory.Product.Insert(project, "id", 4, "name", "p4", "rating", 4.4)
	tests.Factory.Product.Insert(project, "id", 5, "name", "p5", "rating", 5.5)
	tests.Factory.Product.Insert(project, "id", 6, "name", "p6", "rating", 6.6)

	res := request.ReqT(t, project.Env()).
		Body(map[string]any{
			"target":     "products",
			"set":        map[string]any{"name": "?1", "rating": "?2"},
			"where":      []any{[]string{"id", ">", "?3"}},
			"order":      []any{"id"},
			"limit":      2,
			"offset":     2,
			"parameters": []any{"px", 9.9, 1},
			"returning":  []any{"id", "name", "rating"},
		}).
		Post(Update).
		OK()

	assert.Equal(t, res.Body, `{"r":[{"id":4,"name":"px","rating":9.9},{"id":5,"name":"px","rating":9.9}]}`)

	rows := getRows(project, "select id, name, rating from products order by id")
	assert.Equal(t, rows[0].Int("id"), 1)
	assert.Equal(t, rows[0].String("name"), "p1")
	assert.Equal(t, rows[0].Float("rating"), 1.1)

	assert.Equal(t, rows[1].Int("id"), 2)
	assert.Equal(t, rows[1].String("name"), "p2")
	assert.Equal(t, rows[1].Float("rating"), 2.2)

	assert.Equal(t, rows[2].Int("id"), 3)
	assert.Equal(t, rows[2].String("name"), "p3")
	assert.Equal(t, rows[2].Float("rating"), 3.3)

	assert.Equal(t, rows[3].Int("id"), 4)
	assert.Equal(t, rows[3].String("name"), "px")
	assert.Equal(t, rows[3].Float("rating"), 9.9)

	assert.Equal(t, rows[4].Int("id"), 5)
	assert.Equal(t, rows[4].String("name"), "px")
	assert.Equal(t, rows[4].Float("rating"), 9.9)

	assert.Equal(t, rows[5].Int("id"), 6)
	assert.Equal(t, rows[5].String("name"), "p6")
	assert.Equal(t, rows[5].Float("rating"), 6.6)
}

func Test_Update_LimitOverMax(t *testing.T) {
	id := tests.Factory.DynamicId()
	project, _ := sqlkite.Projects.Get(id)
	request.ReqT(t, project.Env()).
		Body(map[string]any{
			"target": "products",
			"set":    map[string]any{"name": "?1", "rating": "?2"},
			"limit":  7,
		}).
		Post(Update).
		ExpectValidation("limit", codes.VAL_SQL_LIMIT_TOO_HIGH)
}
