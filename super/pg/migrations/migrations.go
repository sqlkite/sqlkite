package migrations

import (
	"src.goblgobl.com/utils/pg"
)

func Run(db pg.DB) error {
	migrations := []pg.Migration{
		pg.Migration{Migrate_0001, 1},
	}
	return pg.MigrateAll(db, "sqlkite", migrations)
}

func GetCurrent(db pg.DB) (int, error) {
	return pg.GetCurrentMigrationVersion(db, "sqlkite")
}
