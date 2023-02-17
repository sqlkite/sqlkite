package sqlkite

import (
	"os"
	"path"
	"testing"

	"src.goblgobl.com/tests/assert"
	"src.sqlkite.com/sqlkite/tests"
)

func Test_CreateDB(t *testing.T) {
	id := tests.Generator.UUID()
	dbPath := path.Join(Config.RootPath, id)
	defer os.RemoveAll(dbPath)

	err := CreateDB(id)
	assert.Nil(t, err)

	conn, err := OpenDB(id, false)
	assert.Nil(t, err)
	defer conn.Close()

	rows := conn.Rows("select sql from sqlite_schema where type = 'table' order by name")
	defer rows.Close()

	var createTableSQL string

	assert.True(t, rows.Next())
	rows.Scan(&createTableSQL)
	tests.AssertSQL(t, createTableSQL, `create table sqlkite_sessions (
		id text not null primary key,
		user_id text not null,
		role text null,
		expires int not null,
		created int not null default(unixepoch())
	)`)

	assert.True(t, rows.Next())
	rows.Scan(&createTableSQL)
	tests.AssertSQL(t, createTableSQL, `create table sqlkite_tables (
		name text not null primary key,
		definition text not null,
		created int not null default(unixepoch()),
		updated int not null default(unixepoch())
	)`)

	assert.True(t, rows.Next())
	rows.Scan(&createTableSQL)
	tests.AssertSQL(t, createTableSQL, `create table sqlkite_users (
		id text not null primary key,
		email text not null unique,
		password text not null,
		status int not null,
		role text null,
		created int not null default(unixepoch()),
		updated int not null default(unixepoch())
	)`)

	var journalMode string
	if err := conn.Row("pragma journal_mode").Scan(&journalMode); err != nil {
		panic(err)
	}
	assert.Equal(t, journalMode, "wal")
}
