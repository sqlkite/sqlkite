package projects

import (
	"github.com/valyala/fasthttp"
	"src.goblgobl.com/sqlkite"
	"src.goblgobl.com/sqlkite/data"
	"src.goblgobl.com/sqlkite/super"
	"src.goblgobl.com/utils/http"
	"src.goblgobl.com/utils/typed"
	"src.goblgobl.com/utils/validation"
)

var (
	updateValidation = validation.Object().
		Field("id", idValidation).
		Field("max_concurrency", maxConcurrencyValidation).
		Field("max_sql_length", maxSQLLengthValidation).
		Field("max_sql_parameter_count", maxSQLParameterCountValidation).
		Field("max_database_size", maxDatabaseSizeValidation).
		Field("max_row_count", maxRowCountValidation).
		Field("max_result_length", maxResultLengthValidation)
)

// This is a "global" env: env.Project is nil
func Update(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
	input, err := typed.Json(conn.PostBody())
	if err != nil {
		return http.InvalidJSON, nil
	}

	id := conn.UserValue("id").(string)

	input["id"] = id
	validator := env.Validator
	if !updateValidation.Validate(input, validator) {
		return http.Validation(validator), nil
	}

	ok, err := super.DB.UpdateProject(data.Project{
		Id:                   id,
		MaxConcurrency:       uint16(input.Int("max_concurrency")),
		MaxSQLLength:         uint32(input.Int("max_sql_length")),
		MaxSQLParameterCount: uint16(input.Int("max_sql_parameter_count")),
		MaxDatabaseSize:      uint64(input.Int("max_database_size")),
		MaxRowCount:          uint16(input.Int("max_row_count")),
		MaxResultLength:      uint32(input.Int("max_result_length")),
	})
	if err != nil {
		return nil, err
	}

	if !ok {
		return resNotFound, nil
	}
	return http.Ok(nil), err
}
