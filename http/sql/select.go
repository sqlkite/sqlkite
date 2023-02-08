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
	validator := env.Validator

	columns := selectParseColumns(input[SELECT_INPUT_NAME], validator, project)
	froms := parseFrom(input[FROM_INPUT_NAME], validator, project, true)
	where := parseWhere(input[WHERE_INPUT_NAME], validator)
	orderBy := parseOrderBy(input[ORDER_INPUT_NAME], validator, project)
	limit := parseSelectLimit(input[LIMIT_INPUT_NAME], validator, project)
	offset := parseOffset(input[OFFSET_INPUT_NAME], validator)
	parameters := extractParameters(input[PARAMETERS_INPUT_NAME], validator, project)

	// There's more validation to do, and we do like to return all errors in one
	// shot, but it's possible trying to go further will just cause more problems.
	if !validator.IsValid() {
		return http.Validation(validator), nil
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

	if !validator.IsValid() {
		return http.Validation(validator), nil
	}

	return NewResultResponse(result), nil
}

func selectParseColumns(input any, validator *validation.Result, p *sqlkite.Project) []sql.DataField {
	if input == nil {
		validator.AddInvalidField(selectField, valRequired)
		return nil
	}

	return parseColumnResultList(input, selectField, validator, p)
}

func parseSelectLimit(input any, validator *validation.Result, p *sqlkite.Project) int {
	max := int(p.MaxSelectCount)
	if input == nil {
		return max
	}

	limit, ok := input.(float64)
	if !ok {
		validator.AddInvalidField(limitField, valIntType)
		return 0
	}

	n := int(limit)
	if n > max {
		validator.AddInvalidField(limitField, validation.Invalid{
			Code:  codes.VAL_SQL_LIMIT_TOO_HIGH,
			Error: fmt.Sprintf("limit cannot exceed %d", max),
			Data:  validation.Max(max),
		})
		return 0
	}

	return n
}
