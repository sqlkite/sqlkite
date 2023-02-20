package tests

// This _needs_ to be called "0tests", because we need the init
// in this file to execute before the init in any other file
// (awful)

import (
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	sqliteDriver "src.goblgobl.com/sqlite"
	"src.goblgobl.com/tests/assert"

	"src.goblgobl.com/tests"
	"src.goblgobl.com/utils/argon"
	"src.goblgobl.com/utils/buffer"
	"src.goblgobl.com/utils/log"
	"src.goblgobl.com/utils/typed"
	"src.sqlkite.com/sqlkite/super"
	"src.sqlkite.com/sqlkite/super/pg"
	"src.sqlkite.com/sqlkite/super/sqlite"
)

var Generator = tests.Generator

func init() {
	argon.Insecure()

	err := log.Configure(log.Config{
		Level: "WARN",
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

func CaptureLog(fn func()) string {
	return tests.CaptureLog(fn)
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
	WithDB(func(conn sqliteDriver.Conn) error) error
}

func Row(connProvider ConnProvider, sql string, args ...any) typed.Typed {
	var t typed.Typed
	err := connProvider.WithDB(func(conn sqliteDriver.Conn) error {
		m, err := conn.Row(sql, args...).Map()
		t = typed.Typed(m)
		return err
	})

	if err != nil {
		if err == sqlite.ErrNoRows {
			return nil
		}
		panic(err)
	}

	return t
}

var sqlNormalizePattern = regexp.MustCompile("\\s+")

type BufferWriter interface {
	Write(*buffer.Buffer)
}

// allow either a string to be passed in, or an sql.Part. But since we can't
// reference sql.Part from here, we use a BufferWriter interface.
func AssertSQL(t *testing.T, actual any, expected string) {
	t.Helper()

	var actualString string
	switch typed := actual.(type) {
	case string:
		actualString = typed
	case BufferWriter:
		buf := buffer.New(1024, 1024)
		typed.Write(buf)
		actualString = buf.MustString()
	}

	actualString = sqlNormalizePattern.ReplaceAllString(actualString, " ")
	expected = sqlNormalizePattern.ReplaceAllString(expected, " ")
	assert.Equal(t, strings.ToLower(actualString), strings.ToLower(expected))
}
