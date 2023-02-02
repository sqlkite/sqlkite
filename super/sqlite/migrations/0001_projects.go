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
			max_concurrency int not null,
			max_sql_length int not null,
			max_sql_parameter_count int not null,
			max_database_size int not null,
			max_row_count int not null,
			max_result_length int not null,
			max_from_count int not null,
			max_select_column_count int not null,
			max_condition_count int not null,
			max_order_by_count int not null,
			max_table_count int not null,
			debug bool int null,
			created int not null default(unixepoch()),
			updated int not null default(unixepoch())
	)`)

	if err != nil {
		return fmt.Errorf("sqlite 0001 sqlkite_projects - %w", err)
	}

	return nil
}
