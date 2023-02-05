package sql

import "testing"

func Test_Insert_SingleRow(t *testing.T) {
	assertSQL(t, Insert{
		Into:       "tab1",
		Columns:    []string{"a", "be", "sea"},
		Parameters: []any{1, 2, 3},
	}, "insert into tab1 (a, be, sea) values (?1,?2,?3)")
}

func Test_Insert_MultiRow(t *testing.T) {
	assertSQL(t, Insert{
		Into:       "tab1",
		Columns:    []string{"a", "be", "sea"},
		Parameters: []any{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}, "insert into tab1 (a, be, sea) values (?1,?2,?3) (?4,?5,?6) (?7,?8,?9)")
}
