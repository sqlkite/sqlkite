package main

import (
	"os"
	"strings"

	"src.goblgobl.com/sqlite"
	"src.goblgobl.com/tests"
	"src.goblgobl.com/utils/optional"
	"src.sqlkite.com/sqlkite"
	"src.sqlkite.com/sqlkite/config"
	"src.sqlkite.com/sqlkite/data"
	"src.sqlkite.com/sqlkite/sql"
	"src.sqlkite.com/sqlkite/super"
	"src.sqlkite.com/sqlkite/super/pg"
	superSqlite "src.sqlkite.com/sqlkite/super/sqlite"
)

func main() {
	os.RemoveAll("tests/databases/")
	if err := os.MkdirAll("tests/databases/", 0740); err != nil {
		panic(err)
	}

	config, err := config.Configure("config.test.json")
	if err != nil {
		panic(err)
	}

	if err := sqlkite.Init(config); err != nil {
		panic(err)
	}

	superType := ""
	if args := os.Args; len(args) == 2 {
		superType = args[1]
	}

	var superConfig super.Config
	switch strings.ToLower(superType) {
	case "pg":
		superConfig = super.Config{Postgres: &pg.Config{URL: tests.PG("sqlkite_test")}}
	case "cr":
		superConfig = super.Config{Cockroach: &pg.Config{URL: tests.CR("sqlkite_test")}}
	default:
		superConfig = super.Config{Sqlite: &superSqlite.Config{"tests/sqlkite.super"}}
	}

	if err := super.Configure(superConfig); err != nil {
		panic(err)
	}

	if err := super.DB.EnsureMigrations(); err != nil {
		panic(err)
	}

	setupStandardProject()
	setupDynamicProject()
	setupLimitedProject()
}

func setupStandardProject() {
	id := "00001111-0000-0000-0000-000000000001"
	createProjectDatabase(data.Project{
		Id:                   id,
		MaxConcurrency:       10,
		MaxSQLLength:         2048,
		MaxSQLParameterCount: 10,
		MaxDatabaseSize:      10485763,
		MaxSelectCount:       20,
		MaxResultLength:      524300,
		MaxFromCount:         10,
		MaxSelectColumnCount: 10,
		MaxConditionCount:    10,
		MaxOrderByCount:      5,
		MaxTableCount:        10,
		Debug:                true,
	})

	project := MustGetProject(id)
	err := project.CreateTable(project.Env(), &sql.Table{
		Name: "products",
		Columns: []sql.Column{
			sql.Column{Name: "id", Type: sql.COLUMN_TYPE_INT},
			sql.Column{Name: "name", Type: sql.COLUMN_TYPE_TEXT},
			sql.Column{Name: "rating", Type: sql.COLUMN_TYPE_REAL, Nullable: true},
			sql.Column{Name: "image", Type: sql.COLUMN_TYPE_BLOB, Nullable: true},
		},
	})
	if err != nil {
		panic(err)
	}

	project = MustGetProject(id)
	project.WithDB(func(conn sqlite.Conn) {
		conn.MustExec(`
			insert into products (id, name, rating, image) values
			(?1, ?2, ?3, ?4),
			(?5, ?6, ?7, ?8),
			(?9, ?10, ?11, ?12)
		`,
			1, "KF99", 4.8, nil,
			2, "Absolute", 4.1, []byte{1, 2, 3, 4},
			3, "Keemun", 4.0, []byte{5, 6, 7},
		)
	})

	err = project.CreateTable(project.Env(), &sql.Table{
		Name: "users",
		Columns: []sql.Column{
			sql.Column{Name: "id", Type: sql.COLUMN_TYPE_INT},
			sql.Column{Name: "name", Type: sql.COLUMN_TYPE_TEXT},
			sql.Column{Name: "public", Type: sql.COLUMN_TYPE_INT},
		},
		Access: sql.TableAccess{
			Select: &sql.SelectTableAccess{
				Name: "sqlkite_cte_users",
				CTE:  "select * from users where public = 1",
			},
		},
	})
	if err != nil {
		panic(err)
	}

	project = MustGetProject(id)
	project.WithDB(func(conn sqlite.Conn) {
		conn.MustExec(`
			insert into users (id, name, public) values
			(?1, ?2, ?3),
			(?4, ?5, ?6),
			(?7, ?8, ?9),
			(?10, ?11, ?12)
		`,
			1, "Leto", 0,
			2, "Ghanima", 0,
			3, "Duncan", 1,
			4, "Teg", 1,
		)
	})
}

func setupDynamicProject() {
	id := "00001111-0000-0000-0000-000000000002"
	createProjectDatabase(data.Project{
		Id:                   id,
		MaxConcurrency:       10,
		MaxSQLLength:         2048,
		MaxSQLParameterCount: 10,
		MaxDatabaseSize:      10485763,
		MaxSelectCount:       20,
		MaxResultLength:      524300,
		MaxFromCount:         10,
		MaxSelectColumnCount: 10,
		MaxConditionCount:    10,
		MaxOrderByCount:      5,
		MaxTableCount:        10,
		Debug:                false,
	})

	project := MustGetProject(id)
	// clear out all the tables in this "messy" database
	project.WithDB(func(conn sqlite.Conn) {
		conn.Transaction(func() error {
			rows := conn.Rows("select name from sqlite_schema where type='table' and name not like 'sqlkite_%'")
			defer rows.Close()

			for rows.Next() {
				var name string
				rows.Scan(&name)
				if err := conn.Exec("drop table " + name); err != nil {
					panic(err)
				}
			}
			if err := rows.Error(); err != nil {
				panic(err)
			}
			return nil
		})
	})

	project = MustGetProject(id)
	err := project.CreateTable(project.Env(), &sql.Table{
		Name:           "products",
		MaxDeleteCount: optional.Int(5),
		MaxUpdateCount: optional.Int(6),
		Columns: []sql.Column{
			sql.Column{Name: "id", Type: sql.COLUMN_TYPE_INT},
			sql.Column{Name: "name", Type: sql.COLUMN_TYPE_TEXT},
			sql.Column{Name: "rating", Type: sql.COLUMN_TYPE_REAL, Nullable: true},
			sql.Column{Name: "image", Type: sql.COLUMN_TYPE_BLOB, Nullable: true},
		},
	})
	if err != nil {
		panic(err)
	}
}

func setupLimitedProject() {
	id := "00001111-0000-0000-0000-000000000003"
	createProjectDatabase(data.Project{
		Id:                   id,
		MaxConcurrency:       1,
		MaxSQLLength:         100,
		MaxSQLParameterCount: 2,
		MaxDatabaseSize:      32784,
		MaxSelectCount:       2,
		MaxResultLength:      128,
		MaxFromCount:         2,
		MaxSelectColumnCount: 2,
		MaxConditionCount:    2,
		MaxOrderByCount:      2,
		MaxTableCount:        2,
		Debug:                false,
	})

	project := MustGetProject(id)
	err := project.CreateTable(project.Env(), &sql.Table{
		Name: "t1",
		Columns: []sql.Column{
			sql.Column{Name: "id", Type: sql.COLUMN_TYPE_INT},
			sql.Column{Name: "name", Type: sql.COLUMN_TYPE_TEXT},
		},
	})
	if err != nil {
		panic(err)
	}

	project = MustGetProject(id)
	err = project.CreateTable(project.Env(), &sql.Table{
		Name: "t2",
		Columns: []sql.Column{
			sql.Column{Name: "id", Type: sql.COLUMN_TYPE_INT},
		},
	})
	if err != nil {
		panic(err)
	}
}

func createProjectDatabase(d data.Project) {
	id := d.Id

	if err := sqlkite.CreateDB(id); err != nil {
		panic(err)
	}

	if err := super.DB.DeleteProject(id); err != nil {
		panic(err)
	}

	if err := super.DB.CreateProject(d); err != nil {
		panic(err)
	}
}

func MustGetProject(id string) *sqlkite.Project {
	project, err := sqlkite.Projects.Get(id)
	if err != nil {
		panic(err)
	}
	return project
}
