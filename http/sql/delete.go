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

	vc := env.VC
	project := env.Project

	from := parseRequiredQualifiedTable(input[FROM_INPUT_NAME], fromField, vc)
	table := project.Table(from.Name)
	if table == nil {
		vc.InvalidWithField(sqlkite.UnknownTable(from.Name), fromField)
	}

	where := parseWhere(input[WHERE_INPUT_NAME], vc)
	orderBy := parseOrderBy(input[ORDER_INPUT_NAME], vc, project)
	offset := parseOffset(input[OFFSET_INPUT_NAME], vc)
	parameters := extractParameters(input[PARAMETERS_INPUT_NAME], vc, project)
	returning := parseOptionalColumnResultList(input[RETURNING_INPUT_NAME], returningField, vc, project)

	var limit optional.Int
	if table != nil {
		limit = mutateParseLimit(input[LIMIT_INPUT_NAME], vc, len(returning) > 0, project.Limits.MaxSelectCount, table.MaxDeleteCount)
	}

	// There's more validation to do, and we do like to return all errors in one
	// shot, but it's possible trying to go further will just cause more problems.
	if !vc.IsValid() {
		return http.Validation(vc), nil
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

	if !vc.IsValid() {
		return http.Validation(vc), nil
	}

	return NewResultResponse(result), nil
}
