package sql

import "testing"

func Test_Insert_SingleRow(t *testing.T) {
	assertSQL(t, Insert{
		Into:       Table{"tab1", nil},
		Columns:    []string{"a", "be", "sea"},
		Parameters: []any{1, 2, 3},
	}, "insert into tab1 (a, be, sea) values (?1,?2,?3)")
}

func Test_Insert_MultiRow(t *testing.T) {
	assertSQL(t, Insert{
		Into:       Table{"tab1", &Alias{"x"}},
		Columns:    []string{"a", "be", "sea"},
		Parameters: []any{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}, "insert into tab1 as x (a, be, sea) values (?1,?2,?3), (?4,?5,?6), (?7,?8,?9)")
}

func Test_Insert_With_Returning(t *testing.T) {
	assertSQL(t, Insert{
		Into:       Table{"tab1", nil},
		Columns:    []string{"a"},
		Parameters: []any{1},
		Returning: []DataField{
			DataField{Name: "a"},
		},
	}, "insert into tab1 (a) values (?1) returning json_object('a', a)")

	assertSQL(t, Insert{
		Into:       Table{"tab1", nil},
		Columns:    []string{"a"},
		Parameters: []any{1},
		Returning: []DataField{
			DataField{Name: "a"},
			DataField{Name: "bee", Alias: &Alias{"b"}},
		},
	}, "insert into tab1 (a) values (?1) returning json_object('a', a, 'b', bee)")
}
