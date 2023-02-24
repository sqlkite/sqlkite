package sqlite

import (
	"time"

	"src.goblgobl.com/utils/json"

	"src.goblgobl.com/utils/log"
	"src.goblgobl.com/utils/sqlite"
	"src.sqlkite.com/sqlkite/codes"
	"src.sqlkite.com/sqlkite/data"
	"src.sqlkite.com/sqlkite/super/sqlite/migrations"
)

var ErrNoRows = sqlite.ErrNoRows

type Config struct {
	Path string `json:"path"`
}

type Conn struct {
	sqlite.Conn
}

func New(config Config) (Conn, error) {
	conn, err := sqlite.New(config.Path, true)
	if err != nil {
		return Conn{}, log.Err(codes.ERR_SUPER_SQLITE_NEW, err)
	}
	conn.BusyTimeout(10 * time.Second)
	return Conn{conn}, nil
}

func (c Conn) Ping() error {
	err := c.Exec("select 1")
	if err != nil {
		return log.Err(codes.ERR_SUPER_SQLITE_PING, err)
	}
	return nil
}

func (c Conn) EnsureMigrations() error {
	return migrations.Run(c.Conn)
}

func (c Conn) Info() (any, error) {
	migration, err := sqlite.GetCurrentMigrationVersion(c.Conn)
	if err != nil {
		return nil, err
	}

	return struct {
		Type      string `json:"type"`
		Migration int    `json:"migration"`
	}{
		Type:      "sqlite",
		Migration: migration,
	}, nil
}

func (c Conn) GetProject(id string) (*data.Project, error) {
	row := c.Row(`select data from sqlkite_projects where id = ?1`, id)

	project, err := scanProject(row)
	if err != nil {
		if err == sqlite.ErrNoRows {
			return nil, nil
		}
		return nil, log.Err(codes.ERR_SUPER_SQLITE_GET_PROJECT, err)
	}
	return project, nil
}

func (c Conn) CreateProject(data data.Project) error {
	encoded, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return c.Exec(`insert into sqlkite_projects (id, data) values (?1, ?2)`, data.Id, encoded)
}

func (c Conn) UpdateProject(data data.Project) (bool, error) {
	encoded, err := json.Marshal(data)
	if err != nil {
		return false, err
	}

	err = c.Exec(`update sqlkite_projects set data = ?2, updated = unixepoch() where id = ?1`, data.Id, encoded)

	if err != nil {
		return false, err
	}

	return c.Changes() == 1, nil
}

func (c Conn) DeleteProject(id string) error {
	return c.Exec(`delete from sqlkite_projects where id = ?1`, id)
}

func scanProject(scanner sqlite.Scanner) (*data.Project, error) {
	var encoded []byte
	err := scanner.Scan(&encoded)
	if err != nil {
		return nil, err
	}
	var project *data.Project
	err = json.Unmarshal(encoded, &project)
	return project, err
}
