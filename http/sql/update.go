package sql

import (
	"github.com/valyala/fasthttp"
	"src.goblgobl.com/utils/http"
	"src.goblgobl.com/utils/optional"
	"src.goblgobl.com/utils/typed"
	"src.goblgobl.com/utils/validation"
	"src.sqlkite.com/sqlkite"
	"src.sqlkite.com/sqlkite/codes"
	"src.sqlkite.com/sqlkite/http/sql/parser"
	"src.sqlkite.com/sqlkite/sql"
)

func Update(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
	input, err := typed.Json(conn.PostBody())
	if err != nil {
		return http.InvalidJSON, nil
	}

	project := env.Project
	vc := env.VC

	target := parseRequiredQualifiedTable(input[TARGET_INPUT_NAME], targetField, vc)
	table := project.Table(target.Name)
	if table == nil {
		vc.InvalidWithField(sqlkite.UnknownTable(target.Name), targetField)
	}

	parameters := extractParameters(input[PARAMETERS_INPUT_NAME], vc, project)

	froms := parseFrom(input[FROM_INPUT_NAME], vc, project, false)
	where := parseWhere(input[WHERE_INPUT_NAME], vc)
	orderBy := parseOrderBy(input[ORDER_INPUT_NAME], vc, project)
	offset := parseOffset(input[OFFSET_INPUT_NAME], vc)
	returning := parseOptionalColumnResultList(input[RETURNING_INPUT_NAME], returningField, vc, project)

	// can't parse these without a valid table
	var set []sql.UpdateSet
	var limit optional.Int
	if table != nil {
		set = updateParseSet(input[SET_INPUT_NAME], vc, table, parameters)
		limit = mutateParseLimit(input[LIMIT_INPUT_NAME], vc, len(returning) > 0, project.Limits.MaxSelectCount, table.MaxUpdateCount)
	}

	// There's more validation to do, and we do like to return all errors in one
	// shot, but it's possible trying to go further will just cause more problems.
	if !vc.IsValid() {
		return http.Validation(vc), nil
	}

	update := sql.Update{
		Target:     target,
		Set:        set,
		Froms:      froms,
		Where:      where,
		OrderBy:    orderBy,
		Limit:      limit,
		Offset:     offset,
		Returning:  returning,
		Parameters: parameters,
	}

	result, err := project.Update(env, update)
	if err != nil {
		return nil, err
	}

	if !vc.IsValid() {
		return http.Validation(vc), nil
	}

	return NewResultResponse(result), nil
}

func updateParseSet(input any, ctx *validation.Context[*sqlkite.Env], table *sqlkite.Table, parameters []any) []sql.UpdateSet {
	if input == nil {
		ctx.InvalidWithField(validation.Required, setField)
		return nil
	}

	m, ok := input.(map[string]any)
	if !ok {
		ctx.InvalidWithField(validation.TypeObject, setField)
		return nil
	}

	// TODO: validate that len(m) <= len(table.Columns)
	// TODO: every Table.Column should have a validation.Field so we aren't building
	// this over and over again. Just gonna get this done first, then look at
	// adding validation, at which point I'll add the field.
	i := 0
	set := make([]sql.UpdateSet, len(m))
	for key, value := range m {
		columnName, err := parser.ColumnString(key)
		if err != nil {
			ctx.InvalidWithField(err, setField)
			continue
		}

		column := table.Column(columnName)
		if column == nil {
			ctx.InvalidWithField(sqlkite.UnknownColumn(columnName), setField)
			continue
		}

		df, err := parser.DataField(value)
		if err != nil {
			ctx.InvalidWithField(err, column.Field)
			continue
		}

		if df.Type == sql.DATA_FIELD_PLACEHOLDER {
			ord := df.Ordinal - 1
			if ord >= len(parameters) {
				ctx.InvalidWithField(&validation.Invalid{
					Code:  codes.VAL_PLACEHOLDER_INDEX_OUT_OF_RANGE,
					Error: "Placeholder index is higher than the number of supplied parameters",
					Data:  validation.ValueData(df.Name),
				}, column.Field)
				continue
			}

			column.Validate(parameters[ord], ctx)
		}

		set[i] = sql.UpdateSet{Column: columnName, Value: df}
		i += 1
	}
	return set
}
