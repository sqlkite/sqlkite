package migrations

import (
	"src.goblgobl.com/utils/sqlite"
)

func Run(conn sqlite.Conn) error {
	migrations := []sqlite.Migration{
		sqlite.Migration{1, Migrate_0001},
	}
	return sqlite.MigrateAll(conn, migrations)
}
