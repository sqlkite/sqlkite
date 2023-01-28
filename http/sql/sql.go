package sql

import (
	"fmt"

	"src.goblgobl.com/sqlkite"
	"src.goblgobl.com/sqlkite/codes"
	"src.goblgobl.com/utils/validation"
)

const (
	FROM_INPUT_NAME       = "from"
	WHERE_INPUT_NAME      = "where"
	ORDER_INPUT_NAME      = "order"
	LIMIT_INPUT_NAME      = "limit"
	OFFSET_INPUT_NAME     = "offset"
	SELECT_INPUT_NAME     = "select"
	PARAMETERS_INPUT_NAME = "parameters"
)

// The validation of this package is different and more messy. We don't
// just validate the input as a whole, but rather validate on a field-by-
// field (and sometimes part-by-part) basis as part of parsing. Or, to
// say it differently, we validate as part of parsing.
var (
	valRequired  = validation.Required()
	valIntType   = validation.InvalidIntType()
	valArrayType = validation.InvalidArrayType()

	fromField       = validation.NewIndexedField(FROM_INPUT_NAME)
	whereField      = validation.NewIndexedField(WHERE_INPUT_NAME)
	orderField      = validation.NewIndexedField(ORDER_INPUT_NAME)
	limitField      = validation.NewIndexedField(LIMIT_INPUT_NAME)
	offsetField     = validation.NewIndexedField(OFFSET_INPUT_NAME)
	selectField     = validation.NewIndexedField(SELECT_INPUT_NAME)
	parametersField = validation.NewIndexedField(PARAMETERS_INPUT_NAME)
)

func extractParameters(input any, validator *validation.Result, p *sqlkite.Project) []any {
	if input == nil {
		return nil
	}

	values, ok := input.([]any)
	if !ok {
		validator.AddInvalidField(parametersField, valArrayType)
	}

	max := int(p.MaxSQLParameterCount)
	if len(values) > max {
		validator.AddInvalidField(parametersField, validation.Invalid{
			Code:  codes.VAL_SQL_TOO_MANY_PARAMETERS,
			Error: fmt.Sprintf("must have no more than %d values", max),
			Data:  validation.Max(max),
		})
	}

	return values
}
