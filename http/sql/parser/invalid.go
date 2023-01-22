package parser

import (
	"src.goblgobl.com/sqlkite/codes"
	"src.goblgobl.com/utils/validation"
)

type InvalidFactory func(p *parser) *validation.Invalid

type InvalidContext struct {
	Context any `json:"context"`
}

func invalidColumn(p *parser) *validation.Invalid {
	return &validation.Invalid{
		Code:  codes.VAL_INVALID_COLUMN_NAME,
		Error: "column name is invalid",
		Data:  InvalidContext{Context: p.input},
	}
}

func invalidColumnType() *validation.Invalid {
	return &validation.Invalid{
		Code:  codes.VAL_INVALID_COLUMN_TYPE,
		Error: "column should be a string",
	}
}

func invalidAlias(p *parser) *validation.Invalid {
	return &validation.Invalid{
		Code:  codes.VAL_INVALID_ALIAS_NAME,
		Error: "alias is invalid",
		Data:  InvalidContext{Context: p.input},
	}
}

func invalidTable(p *parser) *validation.Invalid {
	return &validation.Invalid{
		Code:  codes.VAL_INVALID_TABLE_NAME,
		Error: "table is invalid",
		Data:  InvalidContext{Context: p.input},
	}
}

func invalidPredicate() *validation.Invalid {
	return &validation.Invalid{
		Code:  codes.VAL_INVALID_PREDICATE,
		Error: "predicate is invalid",
	}
}

func invalidPredicateOpType() *validation.Invalid {
	return &validation.Invalid{
		Code:  codes.VAL_INVALID_PREDICATE_OP_TYPE,
		Error: "predicate operation must be a string",
	}
}

func invalidPredicateOp(input string) *validation.Invalid {
	return &validation.Invalid{
		Code:  codes.VAL_INVALID_PREDICATE_OP,
		Error: "predicate operation is invalid",
		Data:  InvalidContext{Context: input},
	}
}

func invalidPredicateLeftType() *validation.Invalid {
	return &validation.Invalid{
		Code:  codes.VAL_INVALID_PREDICATE_LEFT_TYPE,
		Error: "left operand of predicate must be a string",
	}
}

func invalidPredicateLeft(p *parser) *validation.Invalid {
	return &validation.Invalid{
		Code:  codes.VAL_INVALID_PREDICATE_LEFT,
		Error: "left operand of predicate is invalid",
		Data:  InvalidContext{Context: p.input},
	}
}

func invalidPredicateRightType() *validation.Invalid {
	return &validation.Invalid{
		Code:  codes.VAL_INVALID_PREDICATE_RIGHT_TYPE,
		Error: "right operand of predicate must be a string",
	}
}

func invalidPredicateRight(p *parser) *validation.Invalid {
	return &validation.Invalid{
		Code:  codes.VAL_INVALID_PREDICATE_RIGHT,
		Error: "right operand of predicate is invalid",
		Data:  InvalidContext{Context: p.input},
	}
}

func invalidPlaceholder(p *parser) *validation.Invalid {
	return &validation.Invalid{
		Code:  codes.VAL_INVALID_PLACEHOLDER,
		Error: "invalid placeholder",
		Data:  InvalidContext{Context: p.input},
	}
}

func invalidConditionCount() *validation.Invalid {
	return &validation.Invalid{
		Code:  codes.VAL_INVALID_CONDITION_COUNT,
		Error: "invalid condition",
	}
}

func invalidConditionLogical(logical string) *validation.Invalid {
	return &validation.Invalid{
		Code:  codes.VAL_INVALID_CONDITION_LOGICAL,
		Error: "invalid logical operator",
		Data:  InvalidContext{Context: logical},
	}
}

func invalidConditionDepth() *validation.Invalid {
	return &validation.Invalid{
		Code:  codes.VAL_INVALID_CONDITION_DEPTH,
		Error: "maximum condition nesting exceeded",
	}
}

func invalidSelectFrom() *validation.Invalid {
	return &validation.Invalid{
		Code:  codes.VAL_INVALID_SELECT_FROM,
		Error: "invalid from",
	}
}

func invalidSelectFromCount() *validation.Invalid {
	return &validation.Invalid{
		Code:  codes.VAL_INVALID_SELECT_FROM_COUNT,
		Error: "invalid from",
	}
}

func invalidSelectFromTable() *validation.Invalid {
	return &validation.Invalid{
		Code:  codes.VAL_INVALID_SELECT_FROM_TABLE,
		Error: "invalid from table",
	}
}

func invalidSelectFromJoin(input string) *validation.Invalid {
	return &validation.Invalid{
		Code:  codes.VAL_INVALID_SELECT_FROM_JOIN,
		Error: "invalid from join",
		Data:  InvalidContext{Context: input},
	}
}

func invalidSelectFromOn() *validation.Invalid {
	return &validation.Invalid{
		Code:  codes.VAL_INVALID_SELECT_FROM_ON,
		Error: "invalid from join condition",
	}
}

func invalidOrderByType() *validation.Invalid {
	return &validation.Invalid{
		Code:  codes.VAL_INVALID_ORDER_BY_TYPE,
		Error: "order by value should be a column name",
	}
}

func invalidOrderBy(p *parser) *validation.Invalid {
	return &validation.Invalid{
		Code:  codes.VAL_INVALID_ORDER_BY,
		Error: "order by value should be a column name",
		Data:  InvalidContext{Context: p.input},
	}
}
