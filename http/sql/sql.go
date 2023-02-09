package sql

import (
	"fmt"

	"src.goblgobl.com/utils/optional"
	"src.goblgobl.com/utils/validation"
	"src.sqlkite.com/sqlkite"
	"src.sqlkite.com/sqlkite/codes"
	"src.sqlkite.com/sqlkite/http/sql/parser"
	"src.sqlkite.com/sqlkite/sql"
)

const (
	FROM_INPUT_NAME       = "from"
	WHERE_INPUT_NAME      = "where"
	ORDER_INPUT_NAME      = "order"
	LIMIT_INPUT_NAME      = "limit"
	OFFSET_INPUT_NAME     = "offset"
	SELECT_INPUT_NAME     = "select"
	PARAMETERS_INPUT_NAME = "parameters"
	INTO_INPUT_NAME       = "into"
	COLUMNS_INPUT_NAME    = "columns"
	RETURNING_INPUT_NAME  = "returning"
	TARGET_INPUT_NAME     = "target"
	SET_INPUT_NAME        = "set"
)

// The validation of this package is different and more messy. We don't
// just validate the input as a whole, but rather validate on a field-by-
// field (and sometimes part-by-part) basis as part of parsing. Or, to
// say it differently, we validate as part of parsing.
var (
	valRequired   = validation.Required()
	valIntType    = validation.InvalidIntType()
	valArrayType  = validation.InvalidArrayType()
	valObjectType = validation.InvalidObjectType()
	valStringType = validation.InvalidStringType()

	fromField       = validation.NewIndexedField(FROM_INPUT_NAME)
	whereField      = validation.NewIndexedField(WHERE_INPUT_NAME)
	orderField      = validation.NewIndexedField(ORDER_INPUT_NAME)
	limitField      = validation.NewIndexedField(LIMIT_INPUT_NAME)
	offsetField     = validation.NewIndexedField(OFFSET_INPUT_NAME)
	selectField     = validation.NewIndexedField(SELECT_INPUT_NAME)
	parametersField = validation.NewIndexedField(PARAMETERS_INPUT_NAME)
	setField        = validation.NewIndexedField(SET_INPUT_NAME)
	intoField       = validation.NewField(INTO_INPUT_NAME)
	targetField     = validation.NewField(TARGET_INPUT_NAME)
	columnsField    = validation.NewIndexedField(COLUMNS_INPUT_NAME)
	returningField  = validation.NewIndexedField(RETURNING_INPUT_NAME)
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

func parseOptionalColumnResultList(input any, field validation.Field, validator *validation.Result, p *sqlkite.Project) []sql.DataField {
	if input == nil {
		return nil
	}
	return parseColumnResultList(input, field, validator, p)
}

func parseColumnResultList(input any, field validation.Field, validator *validation.Result, p *sqlkite.Project) []sql.DataField {
	rawColumns, ok := input.([]any)
	if !ok {
		validator.AddInvalidField(field, valArrayType)
		return nil
	}

	max := int(p.MaxSelectColumnCount)
	if len(rawColumns) > max {
		validator.AddInvalidField(field, validation.Invalid{
			Code:  codes.VAL_SQL_TOO_MANY_SELECT,
			Error: fmt.Sprintf("must return no more than %d columns", max),
			Data:  validation.Max(max),
		})
		return nil
	}

	validator.BeginArray()
	columns := make([]sql.DataField, len(rawColumns))
	for i, rawColumn := range rawColumns {
		column, err := parser.DataField(rawColumn)
		if err != nil {
			validator.ArrayIndex(i)
			validator.AddInvalidField(field, *err)
		} else {
			columns[i] = column
		}
	}
	validator.EndArray()

	return columns
}

func parseRequiredQualifiedTable(input any, field validation.Field, validator *validation.Result) sql.Table {
	if input == nil {
		validator.AddInvalidField(field, valRequired)
		return sql.Table{}
	}

	table, err := parser.QualifiedTableName(input)
	if err != nil {
		validator.AddInvalidField(field, *err)
		return sql.Table{}
	}
	return table
}

func parseFrom(input any, validator *validation.Result, p *sqlkite.Project, required bool) []sql.JoinableFrom {
	if input == nil {
		if required {
			validator.AddInvalidField(fromField, valRequired)
		}
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

func parseWhere(input any, validator *validation.Result) sql.Condition {
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

func parseOrderBy(input any, validator *validation.Result, p *sqlkite.Project) []sql.OrderBy {
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

func parseOffset(input any, validator *validation.Result) optional.Value[int] {
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

// The limit logic for update/delete is different than for select. For select,
// the's _always_ an enforced limit (project.MaxSelectCount). For update/delete,
// the logic is a bit more complicated.
// Every table has an optional MaxUpdateRow or MaxDeleteRow setting which
// (this is TODO, but definitely something we'll add ASAP). Furthermore, if
// the statement includes a returning clause, then (project.MaxSelectCount) must
// also be considered (we'll take the Min(MaxUpdateRow, MaxSelectCount))
// If the table has no MaxUpdateRow (or MaxDeleteRow) and there's no returning
// the limit is optional
func mutateParseLimit(input any, validator *validation.Result, hasReturning bool, maxSelect uint16, maxMutate optional.Value[int]) optional.Value[int] {
	if input == nil && !hasReturning {
		// No limit specified, and not returning anything, our max limit is going to
		// be whatever maxMutate is, which could be nil/infinite.
		return maxMutate
	}

	max := maxMutate
	hasMax := max.Exists
	if hasReturning {
		// there's a returning statement, our max will be the min of maxMutate and maxSelect
		if !hasMax || int(maxSelect) < max.Value {
			hasMax = true
			max = optional.Int(int(maxSelect))
		}
	}

	limit, ok := input.(float64)
	if !ok {
		validator.AddInvalidField(limitField, valIntType)
		return optional.NullInt
	}

	n := int(limit)
	if maxValue := max.Value; hasMax && n > maxValue {
		validator.AddInvalidField(limitField, validation.Invalid{
			Code:  codes.VAL_SQL_LIMIT_TOO_HIGH,
			Error: fmt.Sprintf("limit cannot exceed %d", maxValue),
			Data:  validation.Max(max),
		})
		return optional.NullInt
	}

	return optional.Int(n)
}
