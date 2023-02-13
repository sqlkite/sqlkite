package sql

import (
	"testing"

	"src.sqlkite.com/sqlkite/tests"
)

func Test_Predicate_Write(t *testing.T) {
	tests.AssertSQL(t, Predicate{
		Left:  DataField{Name: "full_name"},
		Op:    []byte(" = "),
		Right: DataField{Name: "?1"},
	}, "full_name = ?1")

	tests.AssertSQL(t, Predicate{
		Left:  DataField{Table: "t2", Name: "id"},
		Op:    []byte(" != "),
		Right: DataField{Table: "t3", Name: "id"},
	}, "t2.id != t3.id")
}

func Test_Condition_Write(t *testing.T) {
	tests.AssertSQL(t, Condition{}, "true")
	// this is more fully tested in http/sql/parser
}
