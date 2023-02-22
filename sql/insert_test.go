package sql

import (
	"testing"

	"src.sqlkite.com/sqlkite/tests"
)

func Test_Insert_SingleRow(t *testing.T) {
	tests.AssertSQL(t, Insert{
		Into:       TableName{Name: "tab1", Alias: nil},
		Columns:    []string{"a", "be", "sea"},
		Parameters: []any{1, 2, 3},
	}, "insert into tab1 (a, be, sea) values (?1,?2,?3)")
}

func Test_Insert_MultiRow(t *testing.T) {
	tests.AssertSQL(t, Insert{
		Into:       TableName{Name: "tab1", Alias: &Alias{"x"}},
		Columns:    []string{"a", "be", "sea"},
		Parameters: []any{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}, "insert into tab1 as x (a, be, sea) values (?1,?2,?3), (?4,?5,?6), (?7,?8,?9)")
}

func Test_Insert_With_Returning(t *testing.T) {
	tests.AssertSQL(t, Insert{
		Into:       TableName{Name: "tab1", Alias: nil},
		Columns:    []string{"a"},
		Parameters: []any{1},
		Returning: []DataField{
			DataField{Name: "a"},
		},
	}, "insert into tab1 (a) values (?1) returning json_object('a', a)")

	tests.AssertSQL(t, Insert{
		Into:       TableName{Name: "tab1", Alias: nil},
		Columns:    []string{"a"},
		Parameters: []any{1},
		Returning: []DataField{
			DataField{Name: "a"},
			DataField{Name: "bee", Alias: &Alias{"b"}},
		},
	}, "insert into tab1 (a) values (?1) returning json_object('a', a, 'b', bee)")
}
