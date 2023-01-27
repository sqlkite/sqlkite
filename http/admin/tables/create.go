package tables

import (
	"github.com/valyala/fasthttp"
	"src.goblgobl.com/sqlkite"
	"src.goblgobl.com/sqlkite/data"
	"src.goblgobl.com/utils/http"
	"src.goblgobl.com/utils/typed"
	"src.goblgobl.com/utils/validation"
)

var (
	createValidation = validation.Object().
		Field("name", tableNameValidation).
		Field("columns", columnsValidation).
		Field("access", accessValidation)
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

	err = env.Project.CreateTable(env, data.Table{
		Name:    name,
		Access:  access,
		Columns: columns,
	})

	// possible that CreateTable added validation errors
	if !validator.IsValid() {
		return http.Validation(validator), nil
	}

	return http.Ok(nil), err
}

func mapColumns(input []typed.Typed) []data.Column {
	columns := make([]data.Column, len(input))
	for i, ci := range input {
		columns[i] = data.Column{
			Name:     ci.String("name"),
			Nullable: ci.Bool("nullable"),
			Default:  ci["default"],
			Type:     ci["type"].(data.ColumnType),
		}
	}
	return columns
}

func mapAccess(input typed.Typed) data.TableAccess {
	var access data.TableAccess
	if input == nil {
		return access
	}

	if cte, ok := input.StringIf("select"); ok {
		access.Select = &data.SelectTableAccess{CTE: cte}
	}

	return access
}
