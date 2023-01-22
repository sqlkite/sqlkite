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
		Field("name", nameValidation).
		Field("columns", columnsValidation)
)

// This is a "global" env: env.Project is nil
func Create(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
	input, err := typed.Json(conn.PostBody())
	if err != nil {
		return http.InvalidJSON, nil
	}

	validator := env.Validator
	if !createValidation.Validate(input, validator) {
		return http.Validation(validator), nil
	}

	columnsInput := input.Objects("columns")
	columns := make([]data.Column, len(columnsInput))
	for i, ci := range columnsInput {
		columns[i] = data.Column{
			Name:     ci.String("name"),
			Nullable: ci.Bool("nullable"),
			Default:  ci["default"],
			Type:     ci["type"].(data.ColumnType),
		}
	}

	err = env.Project.CreateTable(data.Table{
		Name:    input.String("name"),
		Columns: columns,
	})

	return http.Ok(nil), err
}
