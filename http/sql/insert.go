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

	vc := env.VC
	project := env.Project

	into := parseRequiredQualifiedTable(input[INTO_INPUT_NAME], intoField, vc)
	table := project.Table(into.Name)
	if table == nil {
		vc.InvalidWithField(sqlkite.UnknownTable(into.Name), intoField)
	}

	columns := insertParseColumns(input[COLUMNS_INPUT_NAME], vc)
	parameters := extractParameters(input[PARAMETERS_INPUT_NAME], vc, project)
	returning := parseOptionalColumnResultList(input[RETURNING_INPUT_NAME], returningField, vc, project)

	// There's more validation to do, and we do like to return all errors in one
	// shot, but it's possible trying to go further will just cause more problems.
	if !vc.IsValid() {
		return http.Validation(vc), nil
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

	if !vc.IsValid() {
		return http.Validation(vc), nil
	}

	return NewResultResponse(result), nil
}

func insertParseColumns(input any, ctx *validation.Context[*sqlkite.Env]) []string {
	if input == nil {
		ctx.InvalidWithField(validation.Required, columnsField)
		return nil
	}

	rawColumns, ok := input.([]any)
	if !ok {
		ctx.InvalidWithField(validation.TypeArray, columnsField)
		return nil
	}

	// TODO: validate that len(rawColumns) <= len(table.Columns)

	ctx.StartArray()
	columns := make([]string, len(rawColumns))
	for i, rawColumn := range rawColumns {
		column, err := parser.Column(rawColumn)
		if err != nil {
			ctx.ArrayIndex(i)
			ctx.InvalidWithField(err, columnsField)
		} else {
			columns[i] = column
		}
	}
	ctx.EndArray()

	return columns
}
