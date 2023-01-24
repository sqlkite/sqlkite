package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"src.goblgobl.com/utils/pg"

	"src.goblgobl.com/sqlkite/data"
	"src.goblgobl.com/sqlkite/super/pg/migrations"
)

type Config struct {
	URL string `json:"url"`
}

type DB struct {
	pg.DB
	tpe string
}

func New(config Config, tpe string) (DB, error) {
	db, err := pg.New(config.URL)
	if err != nil {
		return DB{}, fmt.Errorf("PG.New - %w", err)
	}
	return DB{db, tpe}, nil
}

func (db DB) Ping() error {
	_, err := db.Exec(context.Background(), "select 1")
	if err != nil {
		return fmt.Errorf("PG.Ping - %w", err)
	}
	return nil
}

func (db DB) EnsureMigrations() error {
	return migrations.Run(db.DB)
}

func (db DB) Info() (any, error) {
	migration, err := migrations.GetCurrent(db.DB)
	if err != nil {
		return nil, err
	}

	return struct {
		Type      string `json:"type"`
		Migration int    `json:"migration"`
	}{
		Type:      db.tpe,
		Migration: migration,
	}, nil
}

func (db DB) GetProject(id string) (*data.Project, error) {
	row := db.QueryRow(context.Background(), `
		select id, max_concurrency, max_sql_length, max_sql_parameter_count,
		       max_database_size, max_row_count, max_result_length, max_from_count,
		       max_select_column_count, max_condition_count, max_order_by_count,
		       max_table_count
		from sqlkite_projects
		where id = $1
	`, id)

	project, err := scanProject(row)
	if err != nil {
		if errors.Is(err, pg.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("PG.GetProject - %w", err)
	}
	return project, nil
}

func (db DB) GetUpdatedProjects(timestamp time.Time) ([]*data.Project, error) {
	// Not sure fetching the count upfront really makes much sense.
	// But we do expect this to be 0 almost every time that it's called, so most
	// of the time we're going to be doing a single DB call (either to get the count
	// which returns 0, or to get an empty result set).
	count, err := pg.Scalar[int](db.DB, "select count(*) from sqlkite_projects where updated > $1", timestamp)
	if count == 0 {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("PG.GetUpdatedProjects (count) - %w", err)
	}

	rows, err := db.Query(context.Background(), `
		select id, max_concurrency, max_sql_length, max_sql_parameter_count,
		       max_database_size, max_row_count, max_result_length, max_from_count,
		       max_select_column_count, max_condition_count, max_order_by_count,
		       max_table_count
		from sqlkite_projects where updated > $1
	`, timestamp)
	if err != nil {
		return nil, fmt.Errorf("PG.GetUpdatedProjects (select) - %w", err)
	}
	defer rows.Close()

	projects := make([]*data.Project, 0, count)
	for rows.Next() {
		project, err := scanProject(rows)
		if err != nil {
			return nil, err
		}
		projects = append(projects, project)
	}

	return projects, rows.Err()
}

func (db DB) CreateProject(data data.Project) error {
	_, err := db.Exec(context.Background(), `
		insert into sqlkite_projects (
			id, max_concurrency, max_sql_length, max_sql_parameter_count,
			max_database_size, max_row_count, max_result_length, max_from_count,
			max_select_column_count, max_condition_count, max_order_by_count,
			max_table_count
		)
		values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	`,
		data.Id, data.MaxConcurrency, data.MaxSQLLength, data.MaxSQLParameterCount,
		data.MaxDatabaseSize, data.MaxRowCount, data.MaxResultLength, data.MaxFromCount,
		data.MaxSelectColumnCount, data.MaxConditionCount, data.MaxOrderByCount,
		data.MaxTableCount)

	return err
}

func (db DB) UpdateProject(data data.Project) (bool, error) {
	cmd, err := db.Exec(context.Background(), `
		update sqlkite_projects set
		  max_concurrency = $2,
		  max_sql_length = $3,
		  max_sql_parameter_count = $4,
		  max_database_size = $5,
		  max_row_count = $6,
		  max_result_length = $7,
		  max_from_count = $8,
		  max_select_column_count = $9,
		  max_condition_count = $10,
		  max_order_by_count = $11,
		  max_table_count = $12
		where id = $1
	`,
		data.Id, data.MaxConcurrency, data.MaxSQLLength, data.MaxSQLParameterCount,
		data.MaxDatabaseSize, data.MaxRowCount, data.MaxResultLength, data.MaxFromCount,
		data.MaxSelectColumnCount, data.MaxConditionCount, data.MaxOrderByCount,
		data.MaxTableCount)

	if err != nil {
		return false, err
	}
	return cmd.RowsAffected() == 1, nil
}

func (db DB) DeleteProject(id string) error {
	_, err := db.Exec(context.Background(), `delete from sqlkite_projects where id = $1`, id)
	return err
}

func scanProject(row pg.Row) (*data.Project, error) {
	var project data.Project
	err := row.Scan(&project.Id,
		&project.MaxConcurrency, &project.MaxSQLLength, &project.MaxSQLParameterCount,
		&project.MaxDatabaseSize, &project.MaxRowCount, &project.MaxResultLength, &project.MaxFromCount,
		&project.MaxSelectColumnCount, &project.MaxConditionCount, &project.MaxOrderByCount,
		&project.MaxTableCount)
	return &project, err
}