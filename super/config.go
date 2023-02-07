package super

import (
	"src.sqlkite.com/sqlkite/super/pg"
	"src.sqlkite.com/sqlkite/super/sqlite"
)

type Config struct {
	Sqlite    *sqlite.Config `json:"sqlite"`
	Postgres  *pg.Config     `json:"postgres"`
	Cockroach *pg.Config     `json:"cockroach"`
}
