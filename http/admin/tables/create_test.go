package tables

import (
	"testing"

	"src.goblgobl.com/sqlkite"
	"src.goblgobl.com/sqlkite/tests"
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
