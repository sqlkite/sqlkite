package super

import (
	"testing"

	"src.goblgobl.com/tests"
	"src.goblgobl.com/tests/assert"
	"src.sqlkite.com/sqlkite/super/pg"
	"src.sqlkite.com/sqlkite/super/sqlite"
)

func Test_Configure_InvalidType(t *testing.T) {
	err := Configure(Config{})
	assert.Equal(t, err.Error(), "code: 303003 - storage.type is invalid. Should be one of: postgres, cockroach or sqlite")
}

func Test_Configure_Sqlite(t *testing.T) {
	config := Config{
		Sqlite: &sqlite.Config{Path: ":memory:"},
	}
	err := Configure(config)
	assert.Nil(t, err)
	_, ok := DB.(sqlite.Conn)
	assert.True(t, ok)
}

func Test_Configure_PG(t *testing.T) {
	if tests.StorageType() != "postgres" {
		return
	}
	config := Config{
		Postgres: &pg.Config{URL: tests.PG("sqlkite_test")},
	}
	err := Configure(config)
	assert.Nil(t, err)
	_, ok := DB.(pg.DB)
	assert.True(t, ok)
}
