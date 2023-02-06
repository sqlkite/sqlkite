package sql

import (
	"fmt"

	"github.com/valyala/fasthttp"
	"src.goblgobl.com/sqlkite"
	"src.goblgobl.com/sqlkite/codes"
	"src.goblgobl.com/sqlkite/http/sql/parser"
	"src.goblgobl.com/sqlkite/sql"
	"src.goblgobl.com/utils/http"
	"src.goblgobl.com/utils/optional"
	"src.goblgobl.com/utils/typed"
	"src.goblgobl.com/utils/validation"
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
	froms := selectParseFrom(input[FROM_INPUT_NAME], validator, project)
	where := selectParseWhere(input[WHERE_INPUT_NAME], validator)
	orderBy := selectParseOrderBy(input[ORDER_INPUT_NAME], validator, project)
	limit := selectParseLimit(input[LIMIT_INPUT_NAME], validator, project)
	offset := selectParseOffset(input[OFFSET_INPUT_NAME], validator)
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

func selectParseFrom(input any, validator *validation.Result, p *sqlkite.Project) []sql.JoinableFrom {
	if input == nil {
		validator.AddInvalidField(fromField, valRequired)
		return nil
	}

	rawFroms, ok := input.([]any)
	if !ok {
		validator.AddInvalidField(fromField, valArrayType)
		return nil
	}

	max := int(p.MaxFromCount)
	if len(rawFroms) > max {
		validator.AddInvalidField(fromField, validation.Invalid{
			Code:  codes.VAL_TOO_MANY_FROMS,
			Error: fmt.Sprintf("must have no more than %d froms", max),
			Data:  validation.Max(max),
		})
		return nil
	}

	validator.BeginArray()
	froms := make([]sql.JoinableFrom, len(rawFroms))
	for i, rawFrom := range rawFroms {
		from, err := parser.JoinableFrom(rawFrom)
		if err != nil {
			validator.ArrayIndex(i)
			validator.AddInvalidField(fromField, *err)
		} else {
			froms[i] = from
		}
	}
	validator.EndArray()
	return froms
}

func selectParseWhere(input any, validator *validation.Result) sql.Condition {
	if input == nil {
		return sql.EmptyCondition
	}

	rawWhere, ok := input.([]any)
	if !ok {
		validator.AddInvalidField(whereField, valArrayType)
		return sql.EmptyCondition
	}

	condition, err := parser.Condition(rawWhere, 0)
	if err != nil {
		validator.AddInvalidField(whereField, *err)
	}
	return condition
}

func selectParseOrderBy(input any, validator *validation.Result, p *sqlkite.Project) []sql.OrderBy {
	if input == nil {
		return nil
	}

	rawOrderBy, ok := input.([]any)
	if !ok {
		validator.AddInvalidField(orderField, valArrayType)
		return nil
	}

	max := int(p.MaxOrderByCount)
	if len(rawOrderBy) > max {
		validator.AddInvalidField(orderField, validation.Invalid{
			Code:  codes.VAL_SQL_TOO_MANY_ORDER_BY,
			Error: fmt.Sprintf("must have no more than %d ordering columns", max),
			Data:  validation.Max(max),
		})
		return nil
	}

	validator.BeginArray()
	fields := make([]sql.OrderBy, len(rawOrderBy))
	for i, rawOrderBy := range rawOrderBy {
		orderBy, err := parser.OrderBy(rawOrderBy)
		if err != nil {
			validator.ArrayIndex(i)
			validator.AddInvalidField(orderField, *err)
		} else {
			fields[i] = orderBy
		}
	}
	validator.EndArray()
	return fields
}

func selectParseLimit(input any, validator *validation.Result, p *sqlkite.Project) int {
	max := int(p.MaxRowCount)
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

func selectParseOffset(input any, validator *validation.Result) optional.Value[int] {
	if input == nil {
		return optional.NullInt
	}

	offset, ok := input.(float64)
	if !ok {
		validator.AddInvalidField(offsetField, valIntType)
		return optional.NullInt
	}

	return optional.Int(int(offset))
}
