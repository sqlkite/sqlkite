package sqlite

import (
	"testing"
	"time"

	"src.goblgobl.com/sqlkite/data"
	"src.goblgobl.com/tests/assert"
	"src.goblgobl.com/utils/log"
	"src.goblgobl.com/utils/uuid"
)

func init() {
	// silence migration info
	err := log.Configure(log.Config{
		Level: "WARN",
	})
	if err != nil {
		panic(err)
	}
}

func Test_Ping(t *testing.T) {
	withTestDB(func(conn Conn) {
		assert.Nil(t, conn.Ping())
	})
}

func withTestDB(fn func(conn Conn)) {
	conn, err := New(Config{Path: ":memory:"})
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	if err := conn.EnsureMigrations(); err != nil {
		panic(err)
	}
	fn(conn)
}

func Test_GetProject_Unknown(t *testing.T) {
	withTestDB(func(conn Conn) {
		p, err := conn.GetProject("unknown")
		assert.Nil(t, err)
		assert.Nil(t, p)
	})
}

func Test_GetProject_Success(t *testing.T) {
	withTestDB(func(conn Conn) {
		conn.MustExec(`
			insert into sqlkite_projects (id,
				max_concurrency, max_sql_length, max_sql_parameter_count,
				max_database_size, max_row_count, max_result_length, max_from_count,
				max_select_column_count, max_condition_count, max_order_by_count,
				max_table_count, debug
			)
			values ('p1', 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, true)
		`)
		p, err := conn.GetProject("p1")
		assert.Nil(t, err)
		assert.Equal(t, p.Id, "p1")
		assert.Equal(t, p.MaxConcurrency, 1)
		assert.Equal(t, p.MaxSQLLength, 2)
		assert.Equal(t, p.MaxSQLParameterCount, 3)
		assert.Equal(t, p.MaxDatabaseSize, 4)
		assert.Equal(t, p.MaxRowCount, 5)
		assert.Equal(t, p.MaxResultLength, 6)
		assert.Equal(t, p.MaxFromCount, 7)
		assert.Equal(t, p.MaxSelectColumnCount, 8)
		assert.Equal(t, p.MaxConditionCount, 9)
		assert.Equal(t, p.MaxOrderByCount, 10)
		assert.Equal(t, p.Debug, true)
	})
}

func Test_GetUpdatedProjects_None(t *testing.T) {
	withTestDB(func(conn Conn) {
		conn.MustExec(`
			insert into sqlkite_projects (id, updated,
				max_concurrency, max_sql_length, max_sql_parameter_count,
				max_database_size, max_row_count, max_result_length, max_from_count,
				max_select_column_count, max_condition_count, max_order_by_count,
				max_table_count, debug
			)
			values ('p1', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, false)
		`)
		updated, err := conn.GetUpdatedProjects(time.Now())
		assert.Nil(t, err)
		assert.Equal(t, len(updated), 0)
	})
}

func Test_GetUpdatedProjects_Success(t *testing.T) {
	withTestDB(func(conn Conn) {
		conn.MustExec(`
			insert into sqlkite_projects (id, updated,
				max_concurrency, max_sql_length, max_sql_parameter_count,
				max_database_size,  max_row_count, max_result_length, max_from_count,
				max_select_column_count, max_condition_count, max_order_by_count,
				max_table_count, debug
			) values
			('p1', unixepoch() - 500, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, false),
			('p2', unixepoch() - 200, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, false),
			('p3', unixepoch() - 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, false),
			('p4', unixepoch() - 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, false)
		`)
		updated, err := conn.GetUpdatedProjects(time.Now().Add(time.Second * -105))
		assert.Nil(t, err)
		assert.Equal(t, len(updated), 2)

		// order isn't deterministic
		id1, id2 := updated[0].Id, updated[1].Id
		assert.True(t, id1 != id2)
		assert.True(t, id1 == "p3" || id1 == "p4")
		assert.True(t, id2 == "p3" || id2 == "p4")
	})
}

func Test_CreateProject(t *testing.T) {
	withTestDB(func(conn Conn) {
		id := uuid.String()
		err := conn.CreateProject(data.Project{
			Id:                   id,
			MaxConcurrency:       10,
			MaxSQLLength:         11,
			MaxSQLParameterCount: 12,
			MaxDatabaseSize:      13,
			MaxRowCount:          14,
			MaxResultLength:      15,
			MaxFromCount:         16,
			MaxSelectColumnCount: 17,
			MaxConditionCount:    18,
			MaxOrderByCount:      19,
			MaxTableCount:        20,
		})
		assert.Nil(t, err)

		project, err := conn.GetProject(id)
		assert.Nil(t, err)
		assert.Equal(t, project.MaxConcurrency, 10)
		assert.Equal(t, project.MaxSQLLength, 11)
		assert.Equal(t, project.MaxSQLParameterCount, 12)
		assert.Equal(t, project.MaxDatabaseSize, 13)
		assert.Equal(t, project.MaxRowCount, 14)
		assert.Equal(t, project.MaxResultLength, 15)
		assert.Equal(t, project.MaxFromCount, 16)
		assert.Equal(t, project.MaxSelectColumnCount, 17)
		assert.Equal(t, project.MaxConditionCount, 18)
		assert.Equal(t, project.MaxOrderByCount, 19)
		assert.Equal(t, project.MaxTableCount, 20)

		// test delete while we're here
		assert.Nil(t, conn.DeleteProject(id))
		project, err = conn.GetProject(id)
		assert.Nil(t, err)
		assert.Nil(t, project)
	})
}

func Test_UpdateProject(t *testing.T) {
	withTestDB(func(conn Conn) {
		id := uuid.String()

		ok, err := conn.UpdateProject(data.Project{Id: id})
		assert.Nil(t, err)
		assert.False(t, ok)

		err = conn.CreateProject(data.Project{Id: id})
		assert.Nil(t, err)

		ok, err = conn.UpdateProject(data.Project{
			Id:                   id,
			MaxConcurrency:       20,
			MaxSQLLength:         21,
			MaxSQLParameterCount: 22,
			MaxDatabaseSize:      23,
			MaxRowCount:          24,
			MaxResultLength:      25,
			MaxFromCount:         26,
			MaxSelectColumnCount: 27,
			MaxConditionCount:    28,
			MaxOrderByCount:      29,
			MaxTableCount:        30,
		})
		assert.Nil(t, err)
		assert.True(t, ok)

		project, err := conn.GetProject(id)
		assert.Nil(t, err)
		assert.Equal(t, project.MaxConcurrency, 20)
		assert.Equal(t, project.MaxSQLLength, 21)
		assert.Equal(t, project.MaxSQLParameterCount, 22)
		assert.Equal(t, project.MaxDatabaseSize, 23)
		assert.Equal(t, project.MaxRowCount, 24)
		assert.Equal(t, project.MaxResultLength, 25)
		assert.Equal(t, project.MaxFromCount, 26)
		assert.Equal(t, project.MaxSelectColumnCount, 27)
		assert.Equal(t, project.MaxConditionCount, 28)
		assert.Equal(t, project.MaxOrderByCount, 29)
		assert.Equal(t, project.MaxTableCount, 30)
	})
}
