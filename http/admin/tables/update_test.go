package tables

import (
	"testing"

	"src.goblgobl.com/sqlkite"
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

func Test_Update_OK(t *testing.T) {
	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body(map[string]any{
			"changes": []any{
				map[string]any{"type": "rename", "to": "test1_name"},
			},
		}).
		UserValue("name", "test1").
		Put(Update).
		OK()
}
