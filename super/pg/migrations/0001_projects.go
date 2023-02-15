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
			data jsonb not null,
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
