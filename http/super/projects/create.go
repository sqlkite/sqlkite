package projects

import (
	"github.com/valyala/fasthttp"
	"src.goblgobl.com/utils/http"
	"src.goblgobl.com/utils/typed"
	"src.goblgobl.com/utils/uuid"
	"src.goblgobl.com/utils/validation"
	"src.sqlkite.com/sqlkite"
	"src.sqlkite.com/sqlkite/data"
	"src.sqlkite.com/sqlkite/super"
)

var (
	createValidation = validation.Object().
		Field("max_concurrency", maxConcurrencyValidation).
		Field("max_sql_length", maxSQLLengthValidation).
		Field("max_sql_parameter_count", maxSQLParameterCountValidation).
		Field("max_database_size", maxDatabaseSizeValidation).
		Field("max_row_count", maxRowCountValidation).
		Field("max_result_length", maxResultLengthValidation)
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

	id := uuid.String()
	if err := sqlkite.CreateDB(id); err != nil {
		return nil, err
	}

	err = super.DB.CreateProject(data.Project{
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

	return http.Ok(struct {
		Id string `json:"id"`
	}{
		Id: id,
	}), nil
}
