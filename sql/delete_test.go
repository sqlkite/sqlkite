package sql

import (
	"testing"

	"src.goblgobl.com/utils/optional"
	"src.sqlkite.com/sqlkite/tests"
)

func Test_Delete_NoWhere_NoLimit(t *testing.T) {
	tests.AssertSQL(t, Delete{
		From: TableName{nil, "table1"},
	}, "delete from table1")
}

func Test_Delete_NoWhere(t *testing.T) {
	tests.AssertSQL(t, Delete{
		From:  TableName{nil, "table1"},
		Limit: optional.NewInt(32),
	}, "delete from table1 limit 32")
}

func Test_Delete_NoLimit(t *testing.T) {
	tests.AssertSQL(t, Delete{
		From: TableName{&Alias{"t1"}, "table1"},
		Where: Condition{
			Parts: []Part{Predicate{
				Left:  DataField{Name: "enabled"},
				Op:    []byte(" = "),
				Right: DataField{Name: "?1"},
			}},
		},
	}, "delete from table1 as t1 where (enabled = ?1)")
}

func Test_Delete_With_Where_And_Limit(t *testing.T) {
	tests.AssertSQL(t, Delete{
		From:  TableName{nil, "table1"},
		Limit: optional.NewInt(1),
		Where: Condition{
			Parts: []Part{Predicate{
				Left:  DataField{Name: "enabled"},
				Op:    []byte(" = "),
				Right: DataField{Name: "?1"},
			}},
		},
	}, "delete from table1 where (enabled = ?1) limit 1")
}

func Test_Delete_Single_Returning(t *testing.T) {
	tests.AssertSQL(t, Delete{
		From:      TableName{nil, "tab1"},
		Returning: []DataField{DataField{Name: "a"}},
	}, "delete from tab1 returning json_object('a', a)")
}

func Test_Delete_Multiple_Returning_OrderLimitOffset(t *testing.T) {
	tests.AssertSQL(t, Delete{
		From:    TableName{nil, "tab1"},
		Limit:   optional.NewInt(4),
		Offset:  optional.NewInt(5),
		OrderBy: []OrderBy{OrderBy{Field: DataField{Name: "id"}}},
		Returning: []DataField{
			DataField{Name: "a"},
			DataField{Name: "bee", Alias: &Alias{"b"}},
		},
	}, "delete from tab1 returning json_object('a', a, 'b', bee) order by id limit 4 offset 5")
}
