package sqlkite

import (
	"errors"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"src.goblgobl.com/sqlite"
	"src.goblgobl.com/sqlkite/config"
	"src.goblgobl.com/sqlkite/data"
	"src.goblgobl.com/utils/buffer"
)

var Config config.Config
var Buffer *buffer.Pool

func init() {
	// This is a hack, but I really dislike go's lack of circular
	// dependency, especially when it comes to organizing tests.
	// So this is me giving up.
	if strings.HasSuffix(os.Args[0], ".test") {
		Buffer = buffer.NewPool(2, 2048, 4096)

		_, b, _, _ := runtime.Caller(0)
		Config.RootPath = filepath.Dir(b) + "/tests/databases/"
	}

	// In non-test, Config is set by the call to Init which
	// is called by main
}

func Init(config config.Config) error {
	rand.Seed(time.Now().UnixNano())

	Buffer = buffer.NewPoolFromConfig(*config.Buffer)

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
			return sqlite.Conn{}, fmt.Errorf("OpenPath.create(%s) - %w", dir, err)
		}
	}

	db, err := sqlite.Open(dbPath, create)
	if err != nil {
		return db, fmt.Errorf("OpenPath.open(%s) - %w", dbPath, err)
	}

	return db, nil
}

func CreateDB(projectId string) error {
	db, err := OpenDB(projectId, true)
	if err != nil {
		return fmt.Errorf("CreateDB(%s) - %w", projectId, err)
	}
	defer db.Close()

	err = db.Exec("pragma journal_mode=wal")
	if err != nil {
		return fmt.Errorf("CreateDB(%s).journal_mode - %w", projectId, err)
	}

	err = db.Exec(`create table sqlkite_tables (
		name text not null primary key,
		definition text not null
	)`)

	if err != nil {
		return fmt.Errorf("CreateDB(%s).sqlkite_tables - %w", projectId, err)
	}

	return nil
}
