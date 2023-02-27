package sql

import (
	"fmt"

	"github.com/valyala/fasthttp"
	"src.goblgobl.com/utils/http"
	"src.goblgobl.com/utils/typed"
	"src.goblgobl.com/utils/validation"
	"src.sqlkite.com/sqlkite"
	"src.sqlkite.com/sqlkite/codes"
	"src.sqlkite.com/sqlkite/http/sql/parser"
	"src.sqlkite.com/sqlkite/sql"
)

var (
	valParameterMultiple = &validation.Invalid{
		Code:  codes.VAL_INSERT_PLACEHOLDER_MULTIPLE,
		Error: "Number of parameters should be a multiple of the number of columns",
	}
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

	parameters := extractParameters(input[PARAMETERS_INPUT_NAME], vc, project)
	returning := parseOptionalColumnResultList(input[RETURNING_INPUT_NAME], returningField, vc, project)

	// can't parse these without a valid table
	var columns []string
	if table != nil {
		columns = insertParseColumns(input[COLUMNS_INPUT_NAME], vc, table, parameters)
	}

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

func insertParseColumns(input any, ctx *validation.Context[*sqlkite.Env], table *sqlkite.Table, parameters []any) []string {
	if input == nil {
		ctx.InvalidWithField(validation.Required, columnsField)
		return nil
	}

	rawColumns, ok := input.([]any)
	if !ok {
		ctx.InvalidWithField(validation.TypeArray, columnsField)
		return nil
	}

	// This API takes 1 or more rows. So the number of parameters should be a
	// multiple of the number of columns.
	parameterLength := len(parameters)
	if parameterLength == 0 || parameterLength%len(rawColumns) != 0 {
		ctx.InvalidWithField(valParameterMultiple, parametersField)
		return nil
	}

	// We're not only going to validate each column, but we're going to validate
	// all the values of that column. Logically, it might make more sense to
	// validate each row at a time, but doing it per-column is more efficient as
	// it means we only have to lookup each column once. It does mean we'll have
	// to play around with setting the validation error index since we'll be
	// jumping from row to row (again, this API allows inserting multiple rows)
	numberOfCols := len(rawColumns)
	numberOfRows := len(parameters) / numberOfCols

	ctx.StartArray()
	columns := make([]string, len(rawColumns))
	for i, rawColumn := range rawColumns {
		columnName, err := parser.Column(rawColumn)
		if err != nil {
			ctx.ArrayIndex(i)
			ctx.InvalidWithField(err, columnsField)
			continue
		}

		column := table.Column(columnName)
		if column == nil {
			ctx.ArrayIndex(i)
			ctx.InvalidWithField(sqlkite.UnknownColumn(columnName), columnsField)
			continue
		}

		if column.DenyInsert {
			ctx.ArrayIndex(i)
			ctx.InvalidWithField(&validation.Invalid{
				Code:  codes.VAL_COLUMN_INSERT_DENY,
				Error: fmt.Sprintf("Inserting into '%s' is not allowed", columnName),
				Data:  validation.ValueData(columnName),
			}, columnsField)
			continue
		}

		contextState := ctx.SuspendArray()
		for row := 0; row < numberOfRows; row++ {
			parameterIndex := row*numberOfCols + i
			column.ValidateInRow(parameters[parameterIndex], ctx, row)
		}
		ctx.ResumeArray(contextState)

		columns[i] = columnName

	}
	ctx.EndArray()

	return columns
}
