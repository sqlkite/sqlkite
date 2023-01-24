package pg

import (
	"os"
	"testing"
	"time"

	"src.goblgobl.com/sqlkite/data"
	"src.goblgobl.com/tests"
	"src.goblgobl.com/tests/assert"
	"src.goblgobl.com/utils/log"
	"src.goblgobl.com/utils/uuid"
)

var db DB

func shouldRunTests() bool {
	tpe := tests.StorageType()
	return tpe == "cockroach" || tpe == "postgres"
}

func TestMain(m *testing.M) {
	if !shouldRunTests() {
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func init() {
	if !shouldRunTests() {
		return
	}

	err := log.Configure(log.Config{
		Level: "WARN",
	})
	if err != nil {
		panic(err)
	}

	url := tests.PG("sqlkite_test")
	tpe := tests.StorageType()
	if tpe == "cockroach" {
		url = tests.CR("sqlkite_test")
	}

	db, err = New(Config{URL: url}, tpe)
	if err != nil {
		panic(err)
	}
	if err := db.EnsureMigrations(); err != nil {
		panic(err)
	}
}

func Test_Ping(t *testing.T) {
	assert.Nil(t, db.Ping())
}

func Test_GetProject_Unknown(t *testing.T) {
	p, err := db.GetProject("76FBFC33-7CB1-447D-8786-C9D370737AA6")
	assert.Nil(t, err)
	assert.Nil(t, p)
}

func Test_GetProject_Success(t *testing.T) {
	id := uuid.String()
	defer cleanupTempProjects()
	db.MustExec(`
		insert into sqlkite_projects (id,
			max_concurrency, max_sql_length, max_sql_parameter_count,
			max_database_size, max_row_count, max_result_length, max_from_count,
			max_select_column_count, max_condition_count, max_order_by_count,
			max_table_count
		)
		values ($1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
	`, id)

	p, err := db.GetProject(id)
	assert.Nil(t, err)
	assert.Equal(t, p.Id, id)
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
	assert.Equal(t, p.MaxTableCount, 11)
}

func Test_GetUpdatedProjects_None(t *testing.T) {
	id := uuid.String()
	defer cleanupTempProjects()
	db.MustExec(`
		insert into sqlkite_projects (id, updated,
			max_concurrency, max_sql_length, max_sql_parameter_count,
			max_database_size, max_row_count, max_result_length, max_from_count,
			max_select_column_count, max_condition_count, max_order_by_count,
			max_table_count
		)
		values ($1, now() - interval '1 second', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
	`, id)
	updated, err := db.GetUpdatedProjects(time.Now())
	assert.Nil(t, err)
	assert.Equal(t, len(updated), 0)
}

func Test_GetUpdatedProjects_Success(t *testing.T) {
	id1, id2, id3, id4 := uuid.String(), uuid.String(), uuid.String(), uuid.String()
	defer cleanupTempProjects()
	db.MustExec(`
		insert into sqlkite_projects (id, updated,
			max_concurrency, max_sql_length, max_sql_parameter_count,
			max_database_size, max_row_count, max_result_length, max_from_count,
			max_select_column_count, max_condition_count, max_order_by_count,
			max_table_count
		) values
		($1, now() - interval '500 second', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
		($2, now() - interval '200 second', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
		($3, now() - interval '100 second', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
		($4, now() - interval '10 second', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
	`, id1, id2, id3, id4)
	updated, err := db.GetUpdatedProjects(time.Now().Add(time.Second * -105))
	assert.Nil(t, err)
	assert.Equal(t, len(updated), 2)

	// order isn't deterministic
	actual1, actual2 := updated[0].Id, updated[1].Id
	assert.True(t, actual1 != actual2)
	assert.True(t, actual1 == id3 || actual1 == id4)
	assert.True(t, actual2 == id3 || actual2 == id4)
}

func Test_CreateProject(t *testing.T) {
	id := uuid.String()
	err := db.CreateProject(data.Project{
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

	project, err := db.GetProject(id)
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
	assert.Nil(t, db.DeleteProject(id))
	project, err = db.GetProject(id)
	assert.Nil(t, err)
	assert.Nil(t, project)
}

func Test_UpdateProject(t *testing.T) {
	id := uuid.String()
	ok, err := db.UpdateProject(data.Project{Id: id})
	assert.Nil(t, err)
	assert.False(t, ok)

	err = db.CreateProject(data.Project{Id: id})
	assert.Nil(t, err)

	ok, err = db.UpdateProject(data.Project{
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

	project, err := db.GetProject(id)
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
}

func cleanupTempProjects() {
	db.MustExec("delete from sqlkite_projects where id::text not like '00001111-%'")
}
