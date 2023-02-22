package migrations

import (
	"src.goblgobl.com/utils/sqlite"
)

func Run(conn sqlite.Conn) error {
	migrations := []sqlite.Migration{
		sqlite.Migration{Migrate_0001, 1},
	}
	return sqlite.MigrateAll(conn, migrations)
}
