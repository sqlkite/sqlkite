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
			Id:    "p1",
			Debug: true,
			Limits: data.Limits{
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
			},
		})
		assert.Nil(t, err)

		p, err := conn.GetProject("p1")
		assert.Nil(t, err)
		assert.Equal(t, p.Id, "p1")
		assert.Equal(t, p.Debug, true)
		assert.Equal(t, p.Limits.MaxConcurrency, 1)
		assert.Equal(t, p.Limits.MaxSQLLength, 2)
		assert.Equal(t, p.Limits.MaxSQLParameterCount, 3)
		assert.Equal(t, p.Limits.MaxDatabaseSize, 4)
		assert.Equal(t, p.Limits.MaxSelectCount, 5)
		assert.Equal(t, p.Limits.MaxResultLength, 6)
		assert.Equal(t, p.Limits.MaxFromCount, 7)
		assert.Equal(t, p.Limits.MaxSelectColumnCount, 8)
		assert.Equal(t, p.Limits.MaxConditionCount, 9)
		assert.Equal(t, p.Limits.MaxOrderByCount, 10)
	})
}

func Test_CreateProject(t *testing.T) {
	withTestDB(func(conn Conn) {
		id := uuid.String()
		err := conn.CreateProject(data.Project{
			Id: id,
			Limits: data.Limits{
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
			},
		})
		assert.Nil(t, err)

		project, err := conn.GetProject(id)
		assert.Nil(t, err)
		assert.Equal(t, project.Limits.MaxConcurrency, 10)
		assert.Equal(t, project.Limits.MaxSQLLength, 11)
		assert.Equal(t, project.Limits.MaxSQLParameterCount, 12)
		assert.Equal(t, project.Limits.MaxDatabaseSize, 13)
		assert.Equal(t, project.Limits.MaxSelectCount, 14)
		assert.Equal(t, project.Limits.MaxResultLength, 15)
		assert.Equal(t, project.Limits.MaxFromCount, 16)
		assert.Equal(t, project.Limits.MaxSelectColumnCount, 17)
		assert.Equal(t, project.Limits.MaxConditionCount, 18)
		assert.Equal(t, project.Limits.MaxOrderByCount, 19)
		assert.Equal(t, project.Limits.MaxTableCount, 20)

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
			Id: id,
			Limits: data.Limits{
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
			},
		})
		assert.Nil(t, err)
		assert.True(t, ok)

		project, err := conn.GetProject(id)
		assert.Nil(t, err)
		assert.Equal(t, project.Limits.MaxConcurrency, 20)
		assert.Equal(t, project.Limits.MaxSQLLength, 21)
		assert.Equal(t, project.Limits.MaxSQLParameterCount, 22)
		assert.Equal(t, project.Limits.MaxDatabaseSize, 23)
		assert.Equal(t, project.Limits.MaxSelectCount, 24)
		assert.Equal(t, project.Limits.MaxResultLength, 25)
		assert.Equal(t, project.Limits.MaxFromCount, 26)
		assert.Equal(t, project.Limits.MaxSelectColumnCount, 27)
		assert.Equal(t, project.Limits.MaxConditionCount, 28)
		assert.Equal(t, project.Limits.MaxOrderByCount, 29)
		assert.Equal(t, project.Limits.MaxTableCount, 30)
	})
}
