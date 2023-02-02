package tests

// This _needs_ to be called "0tests", because we need the init
// in this file to execute before the init in any other file
// (awful)

import (
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"

	sqliteDriver "src.goblgobl.com/sqlite"

	"src.goblgobl.com/sqlkite/super"
	"src.goblgobl.com/sqlkite/super/pg"
	"src.goblgobl.com/sqlkite/super/sqlite"
	"src.goblgobl.com/tests"
	"src.goblgobl.com/utils/log"
	"src.goblgobl.com/utils/typed"
	"src.goblgobl.com/utils/validation"
)

var generator tests.Generator

func init() {
	err := log.Configure(log.Config{
		Level: "WARN",
	})

	if err != nil {
		panic(err)
	}

	err = validation.Configure(validation.Config{
		PoolSize:  1,
		MaxErrors: 10,
	})

	if err != nil {
		panic(err)
	}

	superConfig := super.Config{}
	switch tests.StorageType() {
	case "sqlite":
		superConfig.Sqlite = &sqlite.Config{Path: TestDBRoot() + "/../sqlkite.super"}
	case "postgres":
		superConfig.Postgres = &pg.Config{URL: tests.PG("sqlkite_test")}
	case "cockroach":
		superConfig.Cockroach = &pg.Config{URL: tests.CR("sqlkite_test")}
	default:
		panic("invalid storage type")
	}

	if err := super.Configure(superConfig); err != nil {
		panic(err)
	}

	if err := super.DB.EnsureMigrations(); err != nil {
		panic(err)
	}
}

func String(constraints ...int) string {
	return generator.String(constraints...)
}

func CaptureLog(fn func()) string {
	return tests.CaptureLog(fn)
}

func UUID() string {
	return generator.UUID()
}

func Row(sql string, args ...any) typed.Typed {
	return tests.Row(super.DB.(tests.TestableDB), sql, args...)
}

func Rows(sql string, args ...any) []typed.Typed {
	return tests.Rows(super.DB.(tests.TestableDB), sql, args...)
}

// Some database in tests/database are "static" and created with the
// tests/setup/main.go script. They always begin with "00001111". We
// need to keep those around. But various tests might create their own
// DB (say, to test creating DBs), and those we want to cleanup
func RemoveTempDBs() {
	root := TestDBRoot()
	isSqliteSuper := tests.StorageType() == "sqlite"
	entries, _ := ioutil.ReadDir(root)
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, "00001111-") {
			continue
		}
		if err := os.RemoveAll(path.Join(root, name)); err != nil {
			panic(err)
		}
	}

	if isSqliteSuper {
		super.DB.(sqlite.Conn).MustExec("delete from sqlkite_projects where id not like '00001111-%'")
	}
}

func TestDBRoot() string {
	_, b, _, _ := runtime.Caller(0)
	return filepath.Dir(b) + "/databases/"
}

type ConnProvider interface {
	WithDB(func(conn sqliteDriver.Conn))
}

// query sqlite_master
func SqliteMaster(connProvider ConnProvider, name string, tableName string) string {
	var sql string
	connProvider.WithDB(func(conn sqliteDriver.Conn) {
		query := "select sql from sqlite_master where name = ?1 and tbl_name = ?2"
		if err := conn.Row(query, name, tableName).Scan(&sql); err != nil {
			if err != sqliteDriver.ErrNoRows {
				panic(err)
			}
		}
	})
	return sql
}
