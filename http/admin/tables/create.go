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
		Field("columns", columnsValidation).
		Field("access", accessValidation)
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

	tableName := input.String("name")

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

	var selectAccess *data.SelectAccessControl
	if access, ok := input.ObjectIf("access"); ok {
		if cte, ok := access.StringIf("select"); ok {
			selectAccess = &data.SelectAccessControl{
				CTE:  cte,
				Name: "sqlkite_cte_" + tableName,
			}
		}
	}

	err = env.Project.CreateTable(env, data.Table{
		Name:    tableName,
		Columns: columns,
		Select:  selectAccess,
	})

	// possible CreateTable added validation errors
	if !validator.IsValid() {
		return http.Validation(validator), nil
	}

	return http.Ok(nil), err
}
