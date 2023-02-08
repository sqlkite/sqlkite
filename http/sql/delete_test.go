package sql

import (
	"testing"

	"src.goblgobl.com/tests/assert"
	"src.goblgobl.com/tests/request"
	"src.sqlkite.com/sqlkite"
	"src.sqlkite.com/sqlkite/tests"
)

func Test_Delete_InvalidBody(t *testing.T) {
	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body("nope").
		Post(Delete).
		ExpectInvalid(2003)
}

func Test_Delete_InvalidData(t *testing.T) {
	// required fields
	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body("{}").
		Post(Delete).
		ExpectValidation("from", 1001).ExpectNoValidation("returning", "limit", "offset", "order", "where")

	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body(map[string]any{
			"from":       1,
			"returning":  3.18,
			"where":      3,
			"parameters": 4,
			"limit":      "a",
			"offset":     []int{},
		}).
		Post(Delete).
		ExpectValidation("from", 301_003, "returning", 1011, "where", 1011, "parameters", 1011, "limit", 1005, "offset", 1005)

	request.ReqT(t, standardProject.Env()).
		Body(map[string]any{
			"from":      "",
			"returning": []any{"ok", "$"},
		}).
		Post(Delete).
		ExpectValidation("from", 301_003, "returning.1", 301_001)

	// We don't fully test the parser. The parser has tests for that. We just
	// want to test that we handle parser errors correctly.
	request.ReqT(t, standardProject.Env()).
		Body(map[string]any{
			"from": "$nope",
		}).
		Post(Delete).
		ExpectValidation("from", 301_003)
}

func Test_Delete_InvalidTable(t *testing.T) {
	request.ReqT(t, standardProject.Env()).
		Body(map[string]any{
			"from": "not_a_real_table",
		}).
		Post(Delete).
		ExpectValidation("", 302033)
}

func Test_Delete_OverLimits(t *testing.T) {
	request.ReqT(t, limitedProject.Env()).
		Body(map[string]any{
			"from": "tab1",
			"where": []any{
				[]string{"?1", "=", "?2"},
				[]string{"?1", "=", "?3"},
			},
			"parameters": []any{1, 2, 3},
		}).
		Post(Delete).
		ExpectValidation("parameters", 301_024)
}

func Test_Delete_Nothing(t *testing.T) {
	res := request.ReqT(t, standardProject.Env()).
		Body(map[string]any{
			"from":       "products",
			"where":      []any{[]string{"?1", "=", "?2"}},
			"parameters": []any{1, 0},
		}).
		Post(Delete).
		OK()

	assert.Equal(t, res.Body, `{"affected":0}`)
}

func Test_Delete_AtLimits(t *testing.T) {
	res := request.ReqT(t, limitedProject.Env()).
		Body(map[string]any{
			"from":       "t1",
			"where":      []any{[]string{"?1", "=", "?2"}},
			"parameters": []any{1, 0},
		}).
		Post(Delete).
		OK()

	assert.Equal(t, res.Body, `{"affected":0}`)
}

func Test_Delete_SingleRow(t *testing.T) {
	id := tests.Factory.DynamicId()
	project, _ := sqlkite.Projects.Get(id)

	tests.Factory.Product.Insert(project, "id", 9998, "name", "p1", "rating", 8)
	tests.Factory.Product.Insert(project, "id", 9999, "name", "p2", "rating", 9)

	res := request.ReqT(t, project.Env()).
		Body(map[string]any{
			"from":       "products",
			"where":      []any{[]string{"id", "=", "?1"}},
			"parameters": []any{9999},
		}).
		Post(Delete).
		OK()

	assert.Equal(t, res.Body, `{"affected":1}`)

	rows := getRows(project, "select * from products")
	assert.Equal(t, len(rows), 1)
	assert.Equal(t, rows[0].Int("id"), 9998)
}

func Test_Delete_LimitOffsetReturningOrder(t *testing.T) {
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
			"from":       "products",
			"where":      []any{[]string{"id", ">", "?3"}},
			"order":      []any{"id"},
			"limit":      2,
			"offset":     2,
			"parameters": []any{"px", 9.9, 1},
			"returning":  []any{"id", "name", "rating"},
		}).
		Post(Delete).
		OK()

	assert.Equal(t, res.Body, `{"r":[{"id":4,"name":"p4","rating":4.4},{"id":5,"name":"p5","rating":5.5}]}`)

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

	assert.Equal(t, rows[3].Int("id"), 6)
	assert.Equal(t, rows[3].String("name"), "p6")
	assert.Equal(t, rows[3].Float("rating"), 6.6)
}
