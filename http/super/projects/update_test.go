package projects

import (
	"testing"

	"src.goblgobl.com/tests/assert"
	"src.goblgobl.com/tests/request"
	"src.goblgobl.com/utils/uuid"
	"src.sqlkite.com/sqlkite"
	"src.sqlkite.com/sqlkite/sql"
	"src.sqlkite.com/sqlkite/tests"
)

func Test_Update_InvalidBody(t *testing.T) {
	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body("nope").
		Put(Update).
		ExpectInvalid(2003)
}

func Test_Update_InvalidData(t *testing.T) {
	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body(map[string]any{
			"max_concurrency":         "wrong_type",
			"max_sql_length":          "wrong_type",
			"max_sql_parameter_count": "wrong_type",
			"max_database_size":       "wrong_type",
			"max_row_count":           "wrong_type",
			"max_result_length":       "wrong_type",
		}).
		UserValue("id", "").
		Put(Update).
		ExpectValidation("id", 1010, "max_concurrency", 1005, "max_sql_length", 1005, "max_sql_parameter_count", 1005, "max_database_size", 1005, "max_row_count", 1005, "max_result_length", 1005)

	// values too low
	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body(map[string]any{
			"max_concurrency":         0,
			"max_sql_length":          511,
			"max_sql_parameter_count": -1,
			"max_database_size":       1048575,
			"max_row_count":           0,
			"max_result_length":       1023,
		}).
		UserValue("id", "nope").
		Put(Update).
		ExpectValidation("id", 1010, "max_concurrency", 1008, "max_sql_length", 1008, "max_sql_parameter_count", 1008, "max_database_size", 1008, "max_row_count", 1008, "max_result_length", 1008)

	// values too high
	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body(map[string]any{
			"max_concurrency":         101,
			"max_sql_length":          16385,
			"max_sql_parameter_count": sql.MAX_PARAMETERS + 1,
			"max_database_size":       10485760001,
			"max_row_count":           10001,
			"max_result_length":       5242881,
		}).
		UserValue("id", "Z143BB4A-63B7-4E73-A373-54983BD2E4E6").
		Put(Update).
		ExpectValidation("id", 1010, "max_concurrency", 1008, "max_sql_length", 1008, "max_sql_parameter_count", 1008, "max_database_size", 1008, "max_row_count", 1008, "max_result_length", 1008)
}

func Test_Update_NotFound(t *testing.T) {
	request.ReqT(t, sqlkite.BuildEnv().NoProject().Env()).
		UserValue("id", uuid.String()).
		Put(Update).
		ExpectNotFound()
}

func Test_Update_DefaultInput(t *testing.T) {
	defer tests.RemoveTempDBs()
	projectId := tests.Factory.Project.Insert().String("id")

	request.ReqT(t, sqlkite.BuildEnv().NoProject().Env()).
		UserValue("id", projectId).
		Put(Update).
		OK()

	row := tests.Row("select * from sqlkite_projects where id = $1", projectId)
	assert.Equal(t, row.Int("max_concurrency"), 5)
	assert.Equal(t, row.Int("max_sql_length"), 4096)
	assert.Equal(t, row.Int("max_sql_parameter_count"), 100)
	assert.Equal(t, row.Int("max_database_size"), 104857600)
	assert.Equal(t, row.Int("max_row_count"), 100)
	assert.Equal(t, row.Int("max_result_length"), 524288)
}

func Test_Update_ExplicitInput(t *testing.T) {
	defer tests.RemoveTempDBs()
	projectId := tests.Factory.Project.Insert().String("id")

	request.ReqT(t, sqlkite.BuildEnv().NoProject().Env()).
		Body(map[string]any{
			"max_concurrency":         7,
			"max_sql_length":          4098,
			"max_sql_parameter_count": 109,
			"max_database_size":       104857610,
			"max_row_count":           111,
			"max_result_length":       524212,
		}).
		UserValue("id", projectId).
		Put(Update).
		OK()

	row := tests.Row("select * from sqlkite_projects where id = $1", projectId)
	assert.Equal(t, row.Int("max_concurrency"), 7)
	assert.Equal(t, row.Int("max_sql_length"), 4098)
	assert.Equal(t, row.Int("max_sql_parameter_count"), 109)
	assert.Equal(t, row.Int("max_database_size"), 104857610)
	assert.Equal(t, row.Int("max_row_count"), 111)
	assert.Equal(t, row.Int("max_result_length"), 524212)
}
