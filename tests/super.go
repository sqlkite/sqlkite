package tests

// query the super database

import (
	"src.goblgobl.com/tests"
	"src.goblgobl.com/utils/typed"
	"src.sqlkite.com/sqlkite/super"
)

var Super superQuery

type superQuery struct{}

func (_ superQuery) Row(sql string, args ...any) typed.Typed {
	return tests.Row(super.DB.(tests.TestableDB), sql, args...)
}

func (_ superQuery) Rows(sql string, args ...any) []typed.Typed {
	return tests.Rows(super.DB.(tests.TestableDB), sql, args...)
}
