package super

import (
	"time"

	"src.goblgobl.com/utils/log"
	"src.sqlkite.com/sqlkite/codes"
	"src.sqlkite.com/sqlkite/data"
	"src.sqlkite.com/sqlkite/super/pg"
	"src.sqlkite.com/sqlkite/super/sqlite"
)

// singleton
var DB SuperDB

type SuperDB interface {
	// health check the super db, returns nil if everything is ok
	Ping() error

	// return information about the super db
	Info() (any, error)

	EnsureMigrations() error

	GetProject(id string) (*data.Project, error)
	GetUpdatedProjects(timestamp time.Time) ([]*data.Project, error)
	CreateProject(data data.Project) error
	UpdateProject(data data.Project) (bool, error)
	DeleteProject(id string) error
}

func Configure(config Config) (err error) {
	if c := config.Sqlite; c != nil {
		DB, err = sqlite.New(*c)
	} else if c := config.Postgres; c != nil {
		DB, err = pg.New(*c, "postgres")
	} else if c := config.Cockroach; c != nil {
		DB, err = pg.New(*c, "cockroach")
	} else {
		err = log.Errf(codes.ERR_INVALID_STORAGE_TYPE, "storage.type is invalid. Should be one of: postgres, cockroach or sqlite")
	}
	return
}
