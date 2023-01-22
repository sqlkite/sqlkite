package super

import (
	"src.goblgobl.com/sqlkite/super/pg"
	"src.goblgobl.com/sqlkite/super/sqlite"
)

type Config struct {
	Sqlite    *sqlite.Config `json:"sqlite"`
	Postgres  *pg.Config     `json:"postgres"`
	Cockroach *pg.Config     `json:"cockroach"`
}
