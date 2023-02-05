package sql

import (
	"testing"

	"src.goblgobl.com/utils/optional"
)

func Test_Delete_NoWhere_NoLimit(t *testing.T) {
	assertSQL(t, Delete{
		From: "table1",
	}, "delete from table1")
}

func Test_Delete_NoWhere(t *testing.T) {
	assertSQL(t, Delete{
		From:  "table1",
		Limit: optional.Int(32),
	}, "delete from table1 limit 32")
}

func Test_Delete_NoLimit(t *testing.T) {
	assertSQL(t, Delete{
		From: "table1",
		Where: Condition{
			Parts: []Part{Predicate{
				Left:  DataField{Name: "enabled"},
				Op:    []byte(" = "),
				Right: DataField{Name: "?1"},
			}},
		},
	}, "delete from table1 where (enabled = ?1)")
}

func Test_Delete_With_Where_And_Limit(t *testing.T) {
	assertSQL(t, Delete{
		From:  "table1",
		Limit: optional.Int(1),
		Where: Condition{
			Parts: []Part{Predicate{
				Left:  DataField{Name: "enabled"},
				Op:    []byte(" = "),
				Right: DataField{Name: "?1"},
			}},
		},
	}, "delete from table1 where (enabled = ?1) limit 1")
}
