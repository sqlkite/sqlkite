package sqlkite

import (
	"errors"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"

	"src.goblgobl.com/sqlite"
	"src.goblgobl.com/utils/buffer"
	"src.goblgobl.com/utils/log"
	"src.goblgobl.com/utils/validation"
	"src.sqlkite.com/sqlkite/codes"
	"src.sqlkite.com/sqlkite/config"
	"src.sqlkite.com/sqlkite/data"
)

var Config config.Config
var Buffer *buffer.Pool
var validationContexts *validation.Pool[*Env]

func init() {
	// This is a hack, but I really dislike go's lack of circular
	// dependency, especially when it comes to organizing tests.
	// So this is me giving up.
	if strings.HasSuffix(os.Args[0], ".test") {
		Buffer = buffer.NewPool(2, 2048, 4096)

		// poolSize = 2
		// maxErrors = 20
		validationContexts = validation.NewPool[*Env](2, 50)

		_, b, _, _ := runtime.Caller(0)
		Config.RootPath = filepath.Dir(b) + "/tests/databases/"

	}

	// In non-test, Config is set by the call to Init which
	// is called by main
}

func Init(config config.Config) error {
	Buffer = buffer.NewPoolFromConfig(*config.Buffers)
	validationContexts = validation.NewPool[*Env](config.Validation.PoolSize, config.Validation.MaxErrors)

	Config = config
	return nil
}

func scanProject(scanner sqlite.Scanner) (*data.Project, error) {
	var id string
	err := scanner.Scan(&id)
	if err != nil {
		return nil, err
	}

	return &data.Project{
		Id: id,
	}, nil
}

func DBPath(projectId string) string {
	return path.Join(Config.RootPath, projectId, "main.sqlite")
}

func OpenDB(projectId string, create bool) (sqlite.Conn, error) {
	return OpenPath(DBPath(projectId), create)
}

func OpenPath(dbPath string, create bool) (sqlite.Conn, error) {
	if create {
		dir := path.Dir(dbPath)
		err := os.MkdirAll(dir, 0700)
		if err != nil && !errors.Is(err, fs.ErrExist) {
			return sqlite.Conn{}, log.ErrData(codes.ERR_DB_CREATE_PATH, err, map[string]any{"dir": dir})
		}
	}

	db, err := sqlite.Open(dbPath, create)
	if err != nil {
		return db, log.ErrData(codes.ERR_DB_OPEN, err, map[string]any{"path": dbPath})
	}

	return db, nil
}

func CreateDB(projectId string) error {
	db, err := OpenDB(projectId, true)
	if err != nil {
		return err.(*log.StructuredError).String("pid", projectId)
	}
	defer db.Close()

	err = db.Exec("pragma journal_mode=wal")
	if err != nil {
		return log.ErrData(codes.ERR_PRAGMA_JOURNAL, err, map[string]any{"pid": projectId})
	}

	err = db.Exec(`create table sqlkite_tables (
		name text not null primary key,
		definition text not null,
		created int not null default(unixepoch()),
		updated int not null default(unixepoch())
	)`)

	if err != nil {
		return log.ErrData(codes.ERR_CREATE_SQLKITE_TABLES, err, map[string]any{"pid": projectId})
	}

	err = db.Exec(`create table sqlkite_users (
		id text not null primary key,
		email text not null unique,
		password text not null,
		status int not null,
		role text null,
		created int not null default(unixepoch()),
		updated int not null default(unixepoch())
	)`)

	if err != nil {
		return log.ErrData(codes.ERR_CREATE_SQLKITE_USERS, err, map[string]any{"pid": projectId})
	}

	err = db.Exec(`create table sqlkite_sessions (
		id text not null primary key,
		user_id text not null,
		role text null,
		expires int not null,
		created int not null default(unixepoch())
	)`)
	if err != nil {
		return log.ErrData(codes.ERR_CREATE_SQLKITE_SESSIONS, err, map[string]any{"pid": projectId})
	}

	return nil
}
