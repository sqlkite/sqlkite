package tests

import (
	"time"

	"src.goblgobl.com/sqlite"
	"src.goblgobl.com/sqlkite/super"
	f "src.goblgobl.com/tests/factory"
	"src.goblgobl.com/utils/uuid"
)

var (
	// need to be created in init, after we've loaded out super
	// engine, since the factories can change slightly based on
	// on the super engine (e.g. how placeholders work)
	Factory factory
)

type factory struct {
	Project f.Table

	// See tests/setup/main.go which is run before our tests and gets us into a
	// known state. This does create a dependency between tests and the setup, and
	// between tests themselves, but there's a lot of setup necessary to test most
	// of the important functionality. It's much easier to rely on a static-ish setup
	// and performs much better

	// Most of our tests are going to be run against the standard project
	StandardId string

	// A database with very low limits (used to test reaching/exceeding those limits)
	LimitedId string
}

// This is a "messy" project and it's meant to be used for more "meta" features
// like adding and removing tables. Tests that use this are relatively uncommon.
// This function not only returns the project id, but it also cleans out the
// database to provide each test with a clean slate
func (_ factory) DynamicId() string {
	id := "00001111-0000-0000-0000-000000000002"
	resetDatabase(id)
	return id
}

func init() {
	f.DB = super.DB.(f.SQLStorage)
	Factory.Project = f.NewTable("sqlkite_projects", func(args f.KV) f.KV {
		return f.KV{
			"id":                      args.UUID("id", uuid.String()),
			"max_concurrency":         args.Int("max_concurrency", 2),
			"max_sql_length":          args.Int("max_sql_length", 4000),
			"max_sql_parameter_count": args.Int("max_sql_parameter_count", 100),
			"max_database_size":       args.Int("max_database_size", 100000),
			"max_row_count":           args.Int("max_row_count", 100),
			"max_result_length":       args.Int("max_result_length", 10000),
			"max_from_count":          args.Int("max_from_count", 10),
			"max_select_column_count": args.Int("max_select_column_count", 10),
			"max_condition_count":     args.Int("max_condition_count", 10),
			"max_order_by_count":      args.Int("max_order_by_count", 5),
			"max_table_count":         args.Int("max_table_count", 10),
			"debug":                   args.Bool("debug", false),
			"created":                 args.Time("created", time.Now()),
			"updated":                 args.Time("updated", time.Now()),
		}
	}, "id")

	Factory.StandardId = "00001111-0000-0000-0000-000000000001"
	Factory.LimitedId = "00001111-0000-0000-0000-000000000003"
}

func resetDatabase(id string) {
	// clear out all the tables in this "messy" database
	conn, err := sqlite.Open(TestDBRoot()+"/"+id+"/main.sqlite", false)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	conn.BusyTimeout(5 * time.Second)

	tables := make([]string, 0, 5)
	rows := conn.Rows("select name from sqlite_schema where type='table' and name not like 'sqlkite_%'")
	defer rows.Close()

	for rows.Next() {
		var table string
		rows.Scan(&table)
		tables = append(tables, table)
	}
	if err := rows.Error(); err != nil {
		panic(err)
	}

	for _, table := range tables {
		if err := conn.Exec("drop table " + table); err != nil {
			panic(err)
		}
	}

	if err := conn.Exec("delete from sqlkite_tables"); err != nil {
		panic(err)
	}
}
