package pg

import (
	"context"
	"errors"

	"src.goblgobl.com/utils/log"
	"src.goblgobl.com/utils/pg"

	"src.sqlkite.com/sqlkite/codes"
	"src.sqlkite.com/sqlkite/data"
	"src.sqlkite.com/sqlkite/super/pg/migrations"
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
		return DB{}, log.Err(codes.ERR_SUPER_PG_NEW, err)
	}
	return DB{db, tpe}, nil
}

func (db DB) Ping() error {
	_, err := db.Exec(context.Background(), "select 1")
	if err != nil {
		return log.Err(codes.ERR_SUPER_PG_PING, err)
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
	row := db.QueryRow(context.Background(), `select data from sqlkite_projects where id = $1`, id)

	project, err := scanProject(row)
	if err != nil {
		if errors.Is(err, pg.ErrNoRows) {
			return nil, nil
		}
		return nil, log.Err(codes.ERR_SUPER_PG_GET_PROJECT, err)
	}
	return project, nil
}

func (db DB) CreateProject(data data.Project) error {
	_, err := db.Exec(context.Background(), `insert into sqlkite_projects (id, data) values ($1, $2)`, data.Id, data)
	return err
}

func (db DB) UpdateProject(data data.Project) (bool, error) {
	cmd, err := db.Exec(context.Background(), `update sqlkite_projects set data = $2, updated = now() where id = $1`, data.Id, data)

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
	var project *data.Project
	err := row.Scan(&project)
	return project, err
}
