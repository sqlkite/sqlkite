package sqlite

import (
	"testing"

	"src.goblgobl.com/tests/assert"
	"src.goblgobl.com/utils/log"
	"src.goblgobl.com/utils/uuid"
	"src.sqlkite.com/sqlkite/data"
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
		err := conn.CreateProject(data.Project{
			Id:                   "p1",
			MaxConcurrency:       1,
			MaxSQLLength:         2,
			MaxSQLParameterCount: 3,
			MaxDatabaseSize:      4,
			MaxSelectCount:       5,
			MaxResultLength:      6,
			MaxFromCount:         7,
			MaxSelectColumnCount: 8,
			MaxConditionCount:    9,
			MaxOrderByCount:      10,
			Debug:                true,
		})
		assert.Nil(t, err)

		p, err := conn.GetProject("p1")
		assert.Nil(t, err)
		assert.Equal(t, p.Id, "p1")
		assert.Equal(t, p.MaxConcurrency, 1)
		assert.Equal(t, p.MaxSQLLength, 2)
		assert.Equal(t, p.MaxSQLParameterCount, 3)
		assert.Equal(t, p.MaxDatabaseSize, 4)
		assert.Equal(t, p.MaxSelectCount, 5)
		assert.Equal(t, p.MaxResultLength, 6)
		assert.Equal(t, p.MaxFromCount, 7)
		assert.Equal(t, p.MaxSelectColumnCount, 8)
		assert.Equal(t, p.MaxConditionCount, 9)
		assert.Equal(t, p.MaxOrderByCount, 10)
		assert.Equal(t, p.Debug, true)
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
			MaxSelectCount:       14,
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
		assert.Equal(t, project.MaxSelectCount, 14)
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
			MaxSelectCount:       24,
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
		assert.Equal(t, project.MaxSelectCount, 24)
		assert.Equal(t, project.MaxResultLength, 25)
		assert.Equal(t, project.MaxFromCount, 26)
		assert.Equal(t, project.MaxSelectColumnCount, 27)
		assert.Equal(t, project.MaxConditionCount, 28)
		assert.Equal(t, project.MaxOrderByCount, 29)
		assert.Equal(t, project.MaxTableCount, 30)
	})
}
