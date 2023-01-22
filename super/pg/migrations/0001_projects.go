package migrations

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func Migrate_0001(tx pgx.Tx) error {
	bg := context.Background()
	if _, err := tx.Exec(bg, `
		create table sqlkite_projects (
			id uuid not null primary key,
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
			created timestamptz not null default now(),
			updated timestamptz not null default now()
		)`); err != nil {
		return fmt.Errorf("pg 0001 migration sqlkite_projects - %w", err)
	}

	if _, err := tx.Exec(bg, `
		create index sqlkite_projects_updated on sqlkite_projects(updated)
	`); err != nil {
		return fmt.Errorf("pg 0001 migration sqlkite_projects_updated - %w", err)
	}
	return nil
}
