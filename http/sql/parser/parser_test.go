package parser

import (
	"fmt"
	"testing"

	"src.goblgobl.com/tests/assert"
	"src.sqlkite.com/sqlkite/codes"
	"src.sqlkite.com/sqlkite/tests"
)

func Test_Column_Invalid(t *testing.T) {
	assertInvalid := func(input any, expectedcode uint32) {
		t.Helper()
		_, err := Column(input)
		assert.Equal(t, err.Code, expectedcode)
	}

	assertInvalid(nil, codes.VAL_INVALID_COLUMN_NAME)
	assertInvalid(1, codes.VAL_INVALID_COLUMN_NAME)
	assertInvalid("", codes.VAL_INVALID_COLUMN_NAME)
	assertInvalid("$as", codes.VAL_INVALID_COLUMN_NAME)
	assertInvalid("hello!world", codes.VAL_INVALID_COLUMN_NAME)
	assertInvalid("hello world", codes.VAL_INVALID_COLUMN_NAME)
}

func Test_DataField_Valid(t *testing.T) {
	assertDataField := func(input string, expectedSQL string) {
		t.Helper()
		dataField, err := DataField(input)
		assert.Nil(t, err)
		tests.AssertSQL(t, dataField, expectedSQL)
	}

	assertDataField("?1", "?1")
	assertDataField("?999 ", "?999")
	assertDataField(" id  ", "id")
	assertDataField("  full_name ", "full_name")

	assertDataField("t.id", "t.id")
	assertDataField("table1.full_name", "table1.full_name")

	assertDataField("?3 as name", "?3 as name")
	assertDataField("  ?5  as    other  ", "?5 as other")

	assertDataField("full_name as name", "full_name as name")
	assertDataField("full_name  as    name", "full_name as name")

	assertDataField("t2.full_name as name", "t2.full_name as name")
	assertDataField("t2.full_name  as    name", "t2.full_name as name")
}

func Test_DataField_Invalid(t *testing.T) {
	assertInvalid := func(input string, expectedcode uint32) {
		t.Helper()
		_, err := DataField(input)
		assert.Equal(t, err.Code, expectedcode)
	}

	// invalid placeholders
	assertInvalid("t1.?2", codes.VAL_INVALID_COLUMN_NAME)
	assertInvalid("?", codes.VAL_INVALID_PLACEHOLDER)
	assertInvalid("?a", codes.VAL_INVALID_PLACEHOLDER)
	assertInvalid("?32a", codes.VAL_INVALID_PLACEHOLDER)

	assertInvalid("", codes.VAL_INVALID_COLUMN_NAME)
	assertInvalid("normal&", codes.VAL_INVALID_COLUMN_NAME)
	assertInvalid("t1&.normal", codes.VAL_INVALID_COLUMN_NAME)
	assertInvalid("full-name", codes.VAL_INVALID_COLUMN_NAME)
	assertInvalid("full*name", codes.VAL_INVALID_COLUMN_NAME)
	assertInvalid("t.1", codes.VAL_INVALID_COLUMN_NAME)
	assertInvalid("t.1name", codes.VAL_INVALID_COLUMN_NAME)
	assertInvalid("^.name", codes.VAL_INVALID_COLUMN_NAME)

	assertInvalid("name as", codes.VAL_INVALID_ALIAS_NAME)
	assertInvalid("t.name as ", codes.VAL_INVALID_ALIAS_NAME)
	assertInvalid("t.name as 3> ", codes.VAL_INVALID_ALIAS_NAME)
	assertInvalid("t1.normal as nope!", codes.VAL_INVALID_ALIAS_NAME)

	// as required
	assertInvalid("name n", codes.VAL_INVALID_ALIAS_NAME)
	assertInvalid("t.full_name   name", codes.VAL_INVALID_ALIAS_NAME)
}

func Test_QualifiedTableName_Valid(t *testing.T) {
	assertFrom := func(input any, expectedSQL string) {
		t.Helper()
		from, err := QualifiedTableName(input)
		assert.Nil(t, err)
		tests.AssertSQL(t, from, expectedSQL)
	}

	assertFrom("table1", "table1")
	assertFrom("    table2 ", "table2")
	assertFrom("table1 as t1", "table1 as t1")
	assertFrom("  table3   as    tablethree", "table3 as tablethree")
}

func Test_QualifiedTableName_Invalid(t *testing.T) {
	assertInvalid := func(input any, expectedCode uint32) {
		t.Helper()
		_, err := QualifiedTableName(input)
		assert.Equal(t, err.Code, expectedCode)
	}

	assertInvalid(nil, codes.VAL_INVALID_TABLE_NAME)
	assertInvalid(32, codes.VAL_INVALID_TABLE_NAME)
	assertInvalid("1abc", codes.VAL_INVALID_TABLE_NAME)
	assertInvalid("table1&", codes.VAL_INVALID_TABLE_NAME)
	assertInvalid("abc abc", codes.VAL_INVALID_ALIAS_NAME)
	assertInvalid("table1 as 1", codes.VAL_INVALID_ALIAS_NAME)
	assertInvalid("table1 as t1&", codes.VAL_INVALID_ALIAS_NAME)
}

func Test_JoinableFrom_Valid(t *testing.T) {
	assertFrom := func(input any, expectedSQL string) {
		t.Helper()
		from, err := JoinableFrom(input)
		assert.Nil(t, err)
		tests.AssertSQL(t, from, expectedSQL)
	}

	// single string is treated just like a From
	assertFrom("table1", "table1")
	assertFrom("    table2 ", "table2")
	assertFrom("table1 as t1", "table1 as t1")
	assertFrom("  table3   as    tablethree", "table3 as tablethree")

	for _, join := range []string{"inner", "left", "right", "full"} {
		assertFrom([]any{join, "table1 as t1", []any{[]any{"t1.id", "=", "t2.parent_id"}}}, join+" join table1 as t1 on (t1.id = t2.parent_id)")
	}

	// slightly more complicated condition
	assertFrom([]any{"left", "table1", []any{[]any{"t1.id", "=", "t2.parent_id"}, "or", []any{"x", "!=", "?1"}}}, "left join table1 on (t1.id = t2.parent_id or x != ?1)")
}

func Test_JoinableFrom_Invalid(t *testing.T) {
	assertInvalid := func(input any, expectedCode uint32) {
		t.Helper()
		_, err := JoinableFrom(input)
		assert.Equal(t, err.Code, expectedCode)
	}

	// just the table name (with optional alias), validates just like a From
	assertInvalid("1abc", codes.VAL_INVALID_TABLE_NAME)
	assertInvalid("table1&", codes.VAL_INVALID_TABLE_NAME)
	assertInvalid("abc abc", codes.VAL_INVALID_ALIAS_NAME)
	assertInvalid("table1 as 1", codes.VAL_INVALID_ALIAS_NAME)
	assertInvalid("table1 as t1&", codes.VAL_INVALID_ALIAS_NAME)

	assertInvalid(1, codes.VAL_INVALID_JOINABLE_FROM)
	assertInvalid([]any{}, codes.VAL_INVALID_JOINABLE_FROM_COUNT)
	assertInvalid([]any{"a", "b"}, codes.VAL_INVALID_JOINABLE_FROM_COUNT)
	assertInvalid([]any{"a", "b", "c", "d"}, codes.VAL_INVALID_JOINABLE_FROM_COUNT)

	assertInvalid([]any{"", "table", nil}, codes.VAL_INVALID_JOINABLE_FROM_JOIN)
	assertInvalid([]any{"INNER", "table", nil}, codes.VAL_INVALID_JOINABLE_FROM_JOIN)
	assertInvalid([]any{"other", "table", nil}, codes.VAL_INVALID_JOINABLE_FROM_JOIN)

	assertInvalid([]any{"inner", "1abc", []any{[]any{"a", "=", "true"}}}, codes.VAL_INVALID_TABLE_NAME)
	assertInvalid([]any{"inner", "table1&", []any{[]any{"a", "=", "true"}}}, codes.VAL_INVALID_TABLE_NAME)
	assertInvalid([]any{"inner", "abc abc", []any{[]any{"a", "=", "true"}}}, codes.VAL_INVALID_ALIAS_NAME)
	assertInvalid([]any{"inner", "table1 as 1", []any{[]any{"a", "=", "true"}}}, codes.VAL_INVALID_ALIAS_NAME)
	assertInvalid([]any{"inner", "table1 as t1&", []any{[]any{"a", "=", "true"}}}, codes.VAL_INVALID_ALIAS_NAME)

	assertInvalid([]any{"inner", "tab1", []any{}}, codes.VAL_INVALID_CONDITION_COUNT)
	assertInvalid([]any{"inner", "tab1", []any{[]any{"2a", "=", "?2"}}}, codes.VAL_INVALID_PREDICATE_LEFT)
	assertInvalid([]any{"inner", "tab1", []any{[]any{"a2", "&", "?2"}}}, codes.VAL_INVALID_PREDICATE_OP)
	assertInvalid([]any{"inner", "tab1", []any{[]any{"a2", "=", "4z"}}}, codes.VAL_INVALID_PREDICATE_RIGHT)
}

func Test_Predicate_Valid(t *testing.T) {
	assertPredicate := func(input any, expectedSQL string) {
		t.Helper()
		p, err := predicate(input, 0)
		assert.Nil(t, err)
		tests.AssertSQL(t, p, expectedSQL)
	}

	for _, op := range []any{"=", "!=", ">", "<", ">=", "<=", "is", "is not"} {
		expectedSQL := fmt.Sprintf("left %s right", op)
		assertPredicate([]any{"left", op, "right"}, expectedSQL)
		assertPredicate([]any{"left", fmt.Sprintf(" %s ", op), "right"}, expectedSQL)
		assertPredicate([]any{"left", fmt.Sprintf("  %s   ", op), "right"}, expectedSQL)
	}

	assertPredicate([]any{"name", "=", "?1"}, "name = ?1")
	assertPredicate([]any{"  n1  ", " !=", "  n1  "}, "n1 != n1")
	assertPredicate([]any{"  t_1.n_1  ", " >=  ", " t2.n1"}, "t_1.n_1 >= t2.n1")
	assertPredicate([]any{"?1", "is", "  ?999"}, "?1 is ?999")
}

func Test_Predicate_Invalid(t *testing.T) {
	assertInvalid := func(input any, expectedCode uint32) {
		t.Helper()
		_, err := predicate(input, 0)
		assert.Equal(t, err.Code, expectedCode)
	}
	assertInvalid(23, codes.VAL_INVALID_PREDICATE)
	assertInvalid(nil, codes.VAL_INVALID_PREDICATE)
	assertInvalid([]any{"l", "r"}, codes.VAL_INVALID_CONDITION_COUNT)

	assertInvalid([]any{"l", "EQ", "r"}, codes.VAL_INVALID_PREDICATE_OP)
	assertInvalid([]any{"l", "=a", "r"}, codes.VAL_INVALID_PREDICATE_OP)
	assertInvalid([]any{"l", "a=", "r"}, codes.VAL_INVALID_PREDICATE_OP)
	assertInvalid([]any{"l", "<>", "r"}, codes.VAL_INVALID_PREDICATE_OP)

	assertInvalid([]any{"9", "=", "r"}, codes.VAL_INVALID_PREDICATE_LEFT)
	assertInvalid([]any{"t.a a", "=", "r"}, codes.VAL_INVALID_PREDICATE_LEFT)
	assertInvalid([]any{"1a", "=", "r"}, codes.VAL_INVALID_PREDICATE_LEFT)
	assertInvalid([]any{"t.1a", "=", "r"}, codes.VAL_INVALID_PREDICATE_LEFT)
	assertInvalid([]any{"t^.name", "=", "r"}, codes.VAL_INVALID_PREDICATE_LEFT)
	assertInvalid([]any{"name as n", "=", "r"}, codes.VAL_INVALID_PREDICATE_LEFT)

	assertInvalid([]any{"l", "=", "9"}, codes.VAL_INVALID_PREDICATE_RIGHT)
	assertInvalid([]any{"l", "=", "t.a a"}, codes.VAL_INVALID_PREDICATE_RIGHT)
	assertInvalid([]any{"l", "=", "1a"}, codes.VAL_INVALID_PREDICATE_RIGHT)
	assertInvalid([]any{"l", "=", "t.1a"}, codes.VAL_INVALID_PREDICATE_RIGHT)
	assertInvalid([]any{"l", "=", "t^.name"}, codes.VAL_INVALID_PREDICATE_RIGHT)
	assertInvalid([]any{"l", "=", "name as n"}, codes.VAL_INVALID_PREDICATE_RIGHT)
}

func Test_Condition_Valid(t *testing.T) {
	assertCondition := func(input []any, expectedSQL string) {
		t.Helper()
		c, err := Condition(input, 0)
		assert.Nil(t, err)
		tests.AssertSQL(t, c, expectedSQL)
	}

	assertCondition([]any{
		[]any{"date ", " >=", "?1"},
	}, "(date >= ?1)")

	assertCondition([]any{
		[]any{"date ", " >=", "?1"}, "and", []any{" id", "  !=", " ?2"},
	}, "(date >= ?1 and id != ?2)")

	assertCondition([]any{
		[]any{"date ", " <=", "?1"}, "or", []any{" id", "  is not ", " ?2"},
	}, "(date <= ?1 or id is not ?2)")

	assertCondition([]any{
		[]any{"date ", " <=", "?1"}, "or", []any{
			[]any{" id", "  is not ", " ?2"}, "and", []any{" name", "  = ", " ?3"},
		},
	}, "(date <= ?1 or (id is not ?2 and name = ?3))")
}

func Test_Condition_Invalid(t *testing.T) {
	assertInvalid := func(input []any, expectedCode uint32) {
		t.Helper()
		_, err := predicate(input, 0)
		assert.Equal(t, err.Code, expectedCode)
	}
	assertInvalid(nil, codes.VAL_INVALID_CONDITION_COUNT)
	assertInvalid([]any{}, codes.VAL_INVALID_CONDITION_COUNT)
	assertInvalid([]any{1}, codes.VAL_INVALID_PREDICATE)
	assertInvalid([]any{[]any{"a", "=", "b"}, "and"}, codes.VAL_INVALID_CONDITION_COUNT)
	assertInvalid([]any{[]any{"a", "=", "b"}, "AND", []any{"a", "*", "b"}}, codes.VAL_INVALID_CONDITION_LOGICAL)
	assertInvalid([]any{[]any{"a", "=", "b"}, "and", []any{"a", "=", "32b"}}, codes.VAL_INVALID_PREDICATE_RIGHT)
	assertInvalid([]any{[]any{[]any{[]any{[]any{[]any{}}}}}}, codes.VAL_INVALID_CONDITION_DEPTH)
}

func Test_OrderBy_Valid(t *testing.T) {
	assertOrderBy := func(input string, expectedSQL string) {
		t.Helper()
		dataField, err := OrderBy(input)
		assert.Nil(t, err)
		tests.AssertSQL(t, dataField, expectedSQL)
	}

	assertOrderBy("?1", "?1")
	assertOrderBy("?999 ", "?999")
	assertOrderBy(" id  ", "id")
	assertOrderBy("  full_name ", "full_name")

	assertOrderBy("t.id", "t.id")
	assertOrderBy("table1.full_name", "table1.full_name")

	assertOrderBy("-?1", "?1 desc")
	assertOrderBy("-?999 ", "?999 desc")
	assertOrderBy(" -id  ", "id desc")
	assertOrderBy("  -full_name ", "full_name desc")

	assertOrderBy("-t.id", "t.id desc")
	assertOrderBy("-table1.full_name", "table1.full_name desc")
}

func Test_OrderBy_Invalid(t *testing.T) {
	assertInvalid := func(input string, expectedcode uint32) {
		t.Helper()
		_, err := OrderBy(input)
		assert.Equal(t, err.Code, expectedcode)
	}

	// invalid placeholders
	assertInvalid("t1.?2", codes.VAL_INVALID_ORDER_BY)
	assertInvalid("?", codes.VAL_INVALID_PLACEHOLDER)
	assertInvalid("?a", codes.VAL_INVALID_PLACEHOLDER)
	assertInvalid("?32a", codes.VAL_INVALID_ORDER_BY)

	assertInvalid("", codes.VAL_INVALID_ORDER_BY)
	assertInvalid("normal&", codes.VAL_INVALID_ORDER_BY)
	assertInvalid("t1&.normal", codes.VAL_INVALID_ORDER_BY)
	assertInvalid("full-name", codes.VAL_INVALID_ORDER_BY)
	assertInvalid("full*name", codes.VAL_INVALID_ORDER_BY)
	assertInvalid("t.1", codes.VAL_INVALID_ORDER_BY)
	assertInvalid("t.1name", codes.VAL_INVALID_ORDER_BY)
	assertInvalid("^.name", codes.VAL_INVALID_ORDER_BY)

	assertInvalid("name as x", codes.VAL_INVALID_ORDER_BY)
}
