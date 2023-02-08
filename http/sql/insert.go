package sql

import (
	"github.com/valyala/fasthttp"
	"src.goblgobl.com/utils/http"
	"src.goblgobl.com/utils/typed"
	"src.goblgobl.com/utils/validation"
	"src.sqlkite.com/sqlkite"
	"src.sqlkite.com/sqlkite/http/sql/parser"
	"src.sqlkite.com/sqlkite/sql"
)

func Insert(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
	input, err := typed.Json(conn.PostBody())
	if err != nil {
		return http.InvalidJSON, nil
	}

	project := env.Project
	validator := env.Validator

	into := parseRequiredQualifiedTable(input[INTO_INPUT_NAME], intoField, validator)
	columns := insertParseColumns(input[COLUMNS_INPUT_NAME], validator)
	parameters := extractParameters(input[PARAMETERS_INPUT_NAME], validator, project)
	returning := parseOptionalColumnResultList(input[RETURNING_INPUT_NAME], returningField, validator, project)

	// There's more validation to do, and we do like to return all errors in one
	// shot, but it's possible trying to go further will just cause more problems.
	if !validator.IsValid() {
		return http.Validation(validator), nil
	}

	insert := sql.Insert{
		Into:       into,
		Columns:    columns,
		Parameters: parameters,
		Returning:  returning,
	}

	result, err := project.Insert(env, insert)
	if err != nil {
		return nil, err
	}

	if !validator.IsValid() {
		return http.Validation(validator), nil
	}

	return NewResultResponse(result), nil
}

func insertParseColumns(input any, validator *validation.Result) []string {
	if input == nil {
		validator.AddInvalidField(columnsField, valRequired)
		return nil
	}

	rawColumns, ok := input.([]any)
	if !ok {
		validator.AddInvalidField(columnsField, valArrayType)
		return nil
	}

	// TODO: validate that len(rawColumns) <= len(table.Columns)

	validator.BeginArray()
	columns := make([]string, len(rawColumns))
	for i, rawColumn := range rawColumns {
		column, err := parser.Column(rawColumn)
		if err != nil {
			validator.ArrayIndex(i)
			validator.AddInvalidField(columnsField, *err)
		} else {
			columns[i] = column
		}
	}
	validator.EndArray()

	return columns
}