package sql

import (
	"github.com/valyala/fasthttp"
	"src.goblgobl.com/utils/http"
	"src.goblgobl.com/utils/optional"
	"src.goblgobl.com/utils/typed"
	"src.sqlkite.com/sqlkite"
	"src.sqlkite.com/sqlkite/sql"
)

func Delete(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
	input, err := typed.Json(conn.PostBody())
	if err != nil {
		return http.InvalidJSON, nil
	}

	project := env.Project
	validator := env.Validator

	from := parseRequiredQualifiedTable(input[FROM_INPUT_NAME], fromField, validator)
	table := project.Table(from.Name)
	if table == nil {
		validator.AddInvalidField(fromField, sqlkite.UnknownTable(from.Name))
	}

	where := parseWhere(input[WHERE_INPUT_NAME], validator)
	orderBy := parseOrderBy(input[ORDER_INPUT_NAME], validator, project)
	offset := parseOffset(input[OFFSET_INPUT_NAME], validator)
	parameters := extractParameters(input[PARAMETERS_INPUT_NAME], validator, project)
	returning := parseOptionalColumnResultList(input[RETURNING_INPUT_NAME], returningField, validator, project)

	var limit optional.Value[int]
	if table != nil {
		limit = mutateParseLimit(input[LIMIT_INPUT_NAME], validator, len(returning) > 0, project.MaxSelectCount, table.MaxDeleteCount)
	}

	// There's more validation to do, and we do like to return all errors in one
	// shot, but it's possible trying to go further will just cause more problems.
	if !validator.IsValid() {
		return http.Validation(validator), nil
	}

	del := sql.Delete{
		From:       from,
		Where:      where,
		OrderBy:    orderBy,
		Limit:      limit,
		Offset:     offset,
		Returning:  returning,
		Parameters: parameters,
	}

	result, err := project.Delete(env, del)
	if err != nil {
		return nil, err
	}

	if !validator.IsValid() {
		return http.Validation(validator), nil
	}

	return NewResultResponse(result), nil
}
