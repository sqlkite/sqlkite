package tables

import (
	"github.com/valyala/fasthttp"
	"src.goblgobl.com/utils/http"
	"src.goblgobl.com/utils/optional"
	"src.goblgobl.com/utils/typed"
	"src.goblgobl.com/utils/validation"
	"src.sqlkite.com/sqlkite"
	"src.sqlkite.com/sqlkite/sql"
)

var (
	createValidation = validation.Object().
		Field("name", tableNameValidation).
		Field("columns", columnsValidation).
		Field("access", accessValidation).
		Field("primary_key", primaryKeyValidation).
		Field("max_delete_count", maxMutateCountValidation).
		Field("max_update_count", maxMutateCountValidation)
)

func Create(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
	input, err := typed.Json(conn.PostBody())
	if err != nil {
		return http.InvalidJSON, nil
	}

	validator := env.Validator
	if !createValidation.Validate(input, validator) {
		return http.Validation(validator), nil
	}

	name := input.String("name")
	columns := mapColumns(input.Objects("columns"))
	access := mapAccess(input.Object("access"))

	err = env.Project.CreateTable(env, &sql.Table{
		Name:           name,
		Access:         access,
		Columns:        columns,
		MaxDeleteCount: input.OptionalInt("max_delete_count"),
		MaxUpdateCount: input.OptionalInt("max_update_count"),
		Extended: &sql.TableExtended{
			PrimaryKey: input.Strings("primary_key"),
		},
	})

	// possible that CreateTable added validation errors
	if !validator.IsValid() {
		return http.Validation(validator), nil
	}

	return http.Ok(nil), err
}

func mapColumns(input []typed.Typed) []sql.Column {
	columns := make([]sql.Column, len(input))
	for i, ci := range input {
		columns[i] = mapColumn(ci)
	}
	return columns
}

func mapColumn(input typed.Typed) sql.Column {
	c := sql.Column{
		Name:     input.String("name"),
		Nullable: input.Bool("nullable"),
		Default:  input["default"],
		Type:     input["type"].(sql.ColumnType),
		Extended: new(sql.ColumnExtended),
	}

	if ai, ok := input["autoincrement"].(sql.AutoIncrementType); ok {
		c.Extended.AutoIncrement = optional.New(ai)
	}

	return c
}

func mapAccess(input typed.Typed) sql.TableAccess {
	var access sql.TableAccess
	if input == nil {
		return access
	}

	if cte, ok := input.StringIf("select"); ok {
		access.Select = &sql.SelectTableAccess{CTE: cte}
	}

	if input, ok := input.ObjectIf("insert"); ok {
		access.Insert = &sql.MutateTableAccess{When: input.String("when"), Trigger: input.String("trigger")}
	}
	if input, ok := input.ObjectIf("update"); ok {
		access.Update = &sql.MutateTableAccess{When: input.String("when"), Trigger: input.String("trigger")}
	}
	if input, ok := input.ObjectIf("delete"); ok {
		access.Delete = &sql.MutateTableAccess{When: input.String("when"), Trigger: input.String("trigger")}
	}

	return access
}
