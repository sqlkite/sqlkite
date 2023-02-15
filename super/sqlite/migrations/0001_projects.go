package migrations

import (
	"fmt"

	"src.goblgobl.com/utils/sqlite"
)

// called from within a transaction
func Migrate_0001(conn sqlite.Conn) error {
	err := conn.Exec(`
		create table sqlkite_projects (
			id text not null primary key,
			data text not null,
			debug bool int null,
			created int not null default(unixepoch()),
			updated int not null default(unixepoch())
	)`)

	if err != nil {
		return fmt.Errorf("sqlite 0001 sqlkite_projects - %w", err)
	}

	return nil
}
