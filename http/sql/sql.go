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
	fromField       = validation.BuildField(FROM_INPUT_NAME + ".#")
	whereField      = validation.BuildField(WHERE_INPUT_NAME + ".#")
	orderField      = validation.BuildField(ORDER_INPUT_NAME + ".#")
	limitField      = validation.BuildField(LIMIT_INPUT_NAME + ".#")
	offsetField     = validation.BuildField(OFFSET_INPUT_NAME + ".#")
	selectField     = validation.BuildField(SELECT_INPUT_NAME + ".#")
	parametersField = validation.BuildField(PARAMETERS_INPUT_NAME + ".#")
	setField        = validation.BuildField(SET_INPUT_NAME + ".#")
	intoField       = validation.BuildField(INTO_INPUT_NAME)
	targetField     = validation.BuildField(TARGET_INPUT_NAME)
	columnsField    = validation.BuildField(COLUMNS_INPUT_NAME + ".#")
	returningField  = validation.BuildField(RETURNING_INPUT_NAME + ".#")
)

func extractParameters(input any, ctx *validation.Context[*sqlkite.Env], p *sqlkite.Project) []any {
	if input == nil {
		return nil
	}

	values, ok := input.([]any)
	if !ok {
		ctx.InvalidWithField(validation.TypeArray, parametersField)
	}

	max := int(p.MaxSQLParameterCount)
	if len(values) > max {
		ctx.InvalidWithField(&validation.Invalid{
			Code:  codes.VAL_SQL_TOO_MANY_PARAMETERS,
			Error: fmt.Sprintf("must have no more than %d values", max),
			Data:  validation.MaxData(max),
		}, parametersField)
	}

	return values
}

func parseOptionalColumnResultList(input any, field *validation.Field, ctx *validation.Context[*sqlkite.Env], p *sqlkite.Project) []sql.DataField {
	if input == nil {
		return nil
	}
	return parseColumnResultList(input, field, ctx, p)
}

func parseColumnResultList(input any, field *validation.Field, ctx *validation.Context[*sqlkite.Env], p *sqlkite.Project) []sql.DataField {
	rawColumns, ok := input.([]any)
	if !ok {
		ctx.InvalidWithField(validation.TypeArray, field)
		return nil
	}

	max := int(p.MaxSelectColumnCount)
	if len(rawColumns) > max {
		ctx.InvalidWithField(&validation.Invalid{
			Code:  codes.VAL_SQL_TOO_MANY_SELECT,
			Error: fmt.Sprintf("must return no more than %d columns", max),
			Data:  validation.MaxData(max),
		}, field)
		return nil
	}

	ctx.StartArray()
	columns := make([]sql.DataField, len(rawColumns))
	for i, rawColumn := range rawColumns {
		column, err := parser.DataField(rawColumn)
		if err != nil {
			ctx.ArrayIndex(i)
			ctx.InvalidWithField(err, field)
		} else {
			columns[i] = column
		}
	}
	ctx.EndArray()

	return columns
}

func parseRequiredQualifiedTable(input any, field *validation.Field, ctx *validation.Context[*sqlkite.Env]) sql.TableName {
	if input == nil {
		ctx.InvalidWithField(validation.Required, field)
		return sql.TableName{}
	}

	table, err := parser.QualifiedTableName(input)
	if err != nil {
		ctx.InvalidWithField(err, field)
		return sql.TableName{}
	}
	return table
}

func parseFrom(input any, ctx *validation.Context[*sqlkite.Env], p *sqlkite.Project, required bool) []sql.JoinableFrom {
	if input == nil {
		if required {
			ctx.InvalidWithField(validation.Required, fromField)
		}
		return nil
	}

	rawFroms, ok := input.([]any)
	if !ok {
		ctx.InvalidWithField(validation.TypeArray, fromField)
		return nil
	}

	max := int(p.MaxFromCount)
	if len(rawFroms) > max {
		ctx.InvalidWithField(&validation.Invalid{
			Code:  codes.VAL_TOO_MANY_FROMS,
			Error: fmt.Sprintf("must have no more than %d froms", max),
			Data:  validation.MaxData(max),
		}, fromField)
		return nil
	}

	ctx.StartArray()
	froms := make([]sql.JoinableFrom, len(rawFroms))
	for i, rawFrom := range rawFroms {
		from, err := parser.JoinableFrom(rawFrom)
		if err != nil {
			ctx.ArrayIndex(i)
			ctx.InvalidWithField(err, fromField)
		} else {
			froms[i] = from
		}
	}
	ctx.EndArray()
	return froms
}

func parseWhere(input any, ctx *validation.Context[*sqlkite.Env]) sql.Condition {
	if input == nil {
		return sql.EmptyCondition
	}

	rawWhere, ok := input.([]any)
	if !ok {
		ctx.InvalidWithField(validation.TypeArray, whereField)
		return sql.EmptyCondition
	}

	condition, err := parser.Condition(rawWhere, 0)
	if err != nil {
		ctx.InvalidWithField(err, whereField)
	}
	return condition
}

func parseOrderBy(input any, ctx *validation.Context[*sqlkite.Env], p *sqlkite.Project) []sql.OrderBy {
	if input == nil {
		return nil
	}

	rawOrderBy, ok := input.([]any)
	if !ok {
		ctx.InvalidWithField(validation.TypeArray, orderField)
		return nil
	}

	max := int(p.MaxOrderByCount)
	if len(rawOrderBy) > max {
		ctx.InvalidWithField(&validation.Invalid{
			Code:  codes.VAL_SQL_TOO_MANY_ORDER_BY,
			Error: fmt.Sprintf("must have no more than %d ordering columns", max),
			Data:  validation.MaxData(max),
		}, orderField)
		return nil
	}

	ctx.StartArray()
	fields := make([]sql.OrderBy, len(rawOrderBy))
	for i, rawOrderBy := range rawOrderBy {
		orderBy, err := parser.OrderBy(rawOrderBy)
		if err != nil {
			ctx.ArrayIndex(i)
			ctx.InvalidWithField(err, orderField)
		} else {
			fields[i] = orderBy
		}
	}
	ctx.EndArray()
	return fields
}

func parseOffset(input any, ctx *validation.Context[*sqlkite.Env]) optional.Int {
	if input == nil {
		return optional.NullInt
	}

	offset, ok := input.(float64)
	if !ok {
		ctx.InvalidWithField(validation.TypeInt, offsetField)
		return optional.NullInt
	}

	return optional.NewInt(int(offset))
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
func mutateParseLimit(input any, ctx *validation.Context[*sqlkite.Env], hasReturning bool, maxSelect uint16, maxMutate optional.Int) optional.Int {
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
			max = optional.NewInt(int(maxSelect))
		}
	}

	limit, ok := input.(float64)
	if !ok {
		ctx.InvalidWithField(validation.TypeInt, limitField)
		return optional.NullInt
	}

	n := int(limit)
	if maxValue := max.Value; hasMax && n > maxValue {
		ctx.InvalidWithField(&validation.Invalid{
			Code:  codes.VAL_SQL_LIMIT_TOO_HIGH,
			Error: fmt.Sprintf("limit cannot exceed %d", maxValue),
			Data:  validation.MaxData(maxValue),
		}, limitField)
		return optional.NullInt
	}

	return optional.NewInt(n)
}
