package sql

import (
	"fmt"

	"github.com/valyala/fasthttp"
	"src.goblgobl.com/utils/http"
	"src.goblgobl.com/utils/typed"
	"src.goblgobl.com/utils/validation"
	"src.sqlkite.com/sqlkite"
	"src.sqlkite.com/sqlkite/codes"
	"src.sqlkite.com/sqlkite/sql"
)

var (
	SELECT_ENVELOPE_START        = []byte(`{"r":[`)
	SELECT_ENVELOPE_END          = []byte(`]}`)
	SELECT_ENVELOPE_FIXED_LENGTH = len(SELECT_ENVELOPE_START) + len(SELECT_ENVELOPE_END)
)

func Select(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
	input, err := typed.Json(conn.PostBody())
	if err != nil {
		return http.InvalidJSON, nil
	}

	project := env.Project
	vc := env.VC

	columns := selectParseColumns(input[SELECT_INPUT_NAME], vc, project)
	froms := parseFrom(input[FROM_INPUT_NAME], vc, project, true)
	where := parseWhere(input[WHERE_INPUT_NAME], vc)
	orderBy := parseOrderBy(input[ORDER_INPUT_NAME], vc, project)
	limit := parseSelectLimit(input[LIMIT_INPUT_NAME], vc, project)
	offset := parseOffset(input[OFFSET_INPUT_NAME], vc)
	parameters := extractParameters(input[PARAMETERS_INPUT_NAME], vc, project)

	// There's more validation to do, and we do like to return all errors in one
	// shot, but it's possible trying to go further will just cause more problems.
	if !vc.IsValid() {
		return http.Validation(vc), nil
	}

	sel := sql.Select{
		Columns:    columns,
		Froms:      froms,
		Where:      where,
		Parameters: parameters,
		Limit:      limit,
		Offset:     offset,
		OrderBy:    orderBy,
	}

	result, err := project.Select(env, sel)
	if err != nil {
		return nil, err
	}

	if !vc.IsValid() {
		return http.Validation(vc), nil
	}

	return NewResultResponse(result), nil
}

func selectParseColumns(input any, ctx *validation.Context[*sqlkite.Env], p *sqlkite.Project) []sql.DataField {
	if input == nil {
		ctx.InvalidWithField(validation.Required, selectField)
		return nil
	}

	return parseColumnResultList(input, selectField, ctx, p)
}

func parseSelectLimit(input any, ctx *validation.Context[*sqlkite.Env], p *sqlkite.Project) int {
	max := int(p.MaxSelectCount)
	if input == nil {
		return max
	}

	limit, ok := input.(float64)
	if !ok {
		ctx.InvalidWithField(validation.TypeInt, limitField)
		return 0
	}

	n := int(limit)
	if n > max {
		ctx.InvalidWithField(&validation.Invalid{
			Code:  codes.VAL_SQL_LIMIT_TOO_HIGH,
			Error: fmt.Sprintf("limit cannot exceed %d", max),
			Data:  validation.MaxData(max),
		}, limitField)
		return 0
	}

	return n
}
