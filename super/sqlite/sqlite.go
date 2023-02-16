package sqlite

import (
	"time"

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
	row := c.Row(`
		select id, max_concurrency, max_sql_length, max_sql_parameter_count,
		       max_database_size, max_select_count, max_result_length, max_from_count,
		       max_select_column_count, max_condition_count, max_order_by_count,
		       max_table_count, debug
		from sqlkite_projects
		where id = ?1
	`, id)

	project, err := scanProject(row)
	if err != nil {
		if err == sqlite.ErrNoRows {
			return nil, nil
		}
		return nil, log.Err(codes.ERR_SUPER_SQLITE_GET_PROJECT, err)
	}
	return project, nil
}

func (c Conn) GetUpdatedProjects(timestamp time.Time) ([]*data.Project, error) {
	// Not sure fetching the count upfront really makes much sense.
	// But we do expect this to be 0 almost every time that it's called, so most
	// of the time we're going to be doing a single DB call (either to get the count
	// which returns 0, or to get an empty result set).
	count, err := sqlite.Scalar[int](c.Conn, "select count(*) from sqlkite_projects where updated > ?1", timestamp)
	if err != nil {
		return nil, log.Err(codes.ERR_SUPER_SQLITE_GET_UPDATED_PROJECT_COUNT, err)
	}
	if count == 0 {
		return nil, nil
	}

	rows := c.Rows(`
		select id, max_concurrency, max_sql_length, max_sql_parameter_count,
		       max_database_size, max_select_count, max_result_length, max_from_count
		       max_select_column_count, max_condition_count, max_order_by_count,
		       max_table_count, debug
		from sqlkite_projects
		where updated > ?1
	`, timestamp)
	defer rows.Close()

	projects := make([]*data.Project, 0, count)
	for rows.Next() {
		project, err := scanProject(&rows)
		if err != nil {
			return nil, err
		}
		projects = append(projects, project)
	}

	if err := rows.Error(); err != nil {
		return nil, log.Err(codes.ERR_SUPER_SQLITE_GET_UPDATED_PROJECT, err)
	}

	return projects, nil
}

func (c Conn) CreateProject(data data.Project) error {
	return c.Exec(`
		insert into sqlkite_projects (
			id, max_concurrency, max_sql_length, max_sql_parameter_count,
			max_database_size, max_select_count, max_result_length, max_from_count,
			max_select_column_count, max_condition_count, max_order_by_count,
			max_table_count, debug
		)
		values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
	`,
		data.Id, data.MaxConcurrency, data.MaxSQLLength, data.MaxSQLParameterCount,
		data.MaxDatabaseSize, data.MaxSelectCount, data.MaxResultLength, data.MaxFromCount,
		data.MaxSelectColumnCount, data.MaxConditionCount, data.MaxOrderByCount,
		data.MaxTableCount, data.Debug)
}

func (c Conn) UpdateProject(data data.Project) (bool, error) {
	err := c.Exec(`
		update sqlkite_projects set
		  max_concurrency = ?2,
		  max_sql_length = ?3,
		  max_sql_parameter_count = ?4,
		  max_database_size = ?5,
		  max_select_count = ?6,
		  max_result_length = ?7,
		  max_from_count = ?8,
		  max_select_column_count = ?9,
		  max_condition_count = ?10,
		  max_order_by_count = ?11,
		  max_table_count = ?12,
		  debug = ?13,
		  updated = unixepoch()
		where id = ?1
	`,
		data.Id, data.MaxConcurrency, data.MaxSQLLength, data.MaxSQLParameterCount,
		data.MaxDatabaseSize, data.MaxSelectCount, data.MaxResultLength, data.MaxFromCount,
		data.MaxSelectColumnCount, data.MaxConditionCount, data.MaxOrderByCount,
		data.MaxTableCount, data.Debug)

	if err != nil {
		return false, err
	}

	return c.Changes() == 1, nil
}

func (c Conn) DeleteProject(id string) error {
	return c.Exec(`delete from sqlkite_projects where id = ?1`, id)
}

func scanProject(scanner sqlite.Scanner) (*data.Project, error) {
	var project data.Project
	err := scanner.Scan(&project.Id,
		&project.MaxConcurrency, &project.MaxSQLLength, &project.MaxSQLParameterCount,
		&project.MaxDatabaseSize, &project.MaxSelectCount, &project.MaxResultLength, &project.MaxFromCount,
		&project.MaxSelectColumnCount, &project.MaxConditionCount, &project.MaxOrderByCount,
		&project.MaxTableCount, &project.Debug)
	return &project, err
}
