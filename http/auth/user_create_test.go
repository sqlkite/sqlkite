package auth

import (
	"os"
	"testing"

	"src.goblgobl.com/tests/assert"
	"src.goblgobl.com/tests/request"
	"src.goblgobl.com/utils"
	"src.goblgobl.com/utils/argon"
	"src.sqlkite.com/sqlkite"
	"src.sqlkite.com/sqlkite/codes"
	"src.sqlkite.com/sqlkite/tests"
)

var (
	projectAuthDisabled *sqlkite.Project

	// try to fit every special case in this 1 project
	projectAuthSpecial *sqlkite.Project
)

func TestMain(m *testing.M) {
	var err error

	projectAuthDisabledId := tests.Factory.Project.Insert("auth.disabled", true).String("id")
	if err := sqlkite.CreateDB(projectAuthDisabledId); err != nil {
		panic(err)
	}
	projectAuthDisabled, err = sqlkite.Projects.Get(projectAuthDisabledId)
	if err != nil {
		panic(err)
	}

	projectAuthSpecialId := tests.Factory.Project.Insert("auth.session.session_ttl", 30).String("id")
	if err := sqlkite.CreateDB(projectAuthSpecialId); err != nil {
		panic(err)
	}
	projectAuthSpecial, err = sqlkite.Projects.Get(projectAuthSpecialId)
	if err != nil {
		panic(err)
	}

	code := m.Run()
	tests.RemoveTempDBs()
	os.Exit(code)
}

func Test_UserCreate_InvalidBody(t *testing.T) {
	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body("nope").
		Post(UserCreate).
		ExpectInvalid(utils.RES_INVALID_JSON_PAYLOAD)
}

func Test_UserCreate_InvalidData(t *testing.T) {
	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Post(UserCreate).
		ExpectValidation("email", utils.VAL_REQUIRED, "password", utils.VAL_REQUIRED)

	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body(map[string]any{
			"email":    4,
			"password": 5,
		}).
		Post(UserCreate).
		ExpectValidation("email", utils.VAL_STRING_TYPE, "password", utils.VAL_STRING_TYPE)

	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body(map[string]any{
			"email":    "hello",
			"password": "12345678",
		}).
		Post(UserCreate).
		ExpectValidation("email", utils.VAL_STRING_PATTERN, "password", codes.VAL_COMMON_PASSWORD)

	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body(map[string]any{
			"password": "1234567",
		}).
		Post(UserCreate).
		ExpectValidation("password", utils.VAL_STRING_LENGTH)
}

func Test_UserCreate_Success(t *testing.T) {
	id := tests.Factory.DynamicId()
	project := mustGetProject(id)

	res := request.ReqT(t, project.Env()).
		Body(map[string]any{
			"email":    "Teg@SQLkite.com",
			"password": "ghanima1",
		}).
		Post(UserCreate).
		OK().JSON()

	userId := res.String("id")
	row := tests.Row(project, "select * from sqlkite_users where id = ?1", userId)
	assert.Nil(t, row["role"])
	assert.Equal(t, row.Int("status"), 1)
	assert.Nowish(t, row.Time("created"))
	assert.Nowish(t, row.Time("updated"))
	assert.Equal(t, row.String("email"), "teg@sqlkite.com")

	match, err := argon.Compare("ghanima1", row.String("password"))
	assert.Nil(t, err)
	assert.True(t, match)
}

func Test_UserCreate_DuplicateEmail(t *testing.T) {
	id := tests.Factory.DynamicId()
	project := mustGetProject(id)

	request.ReqT(t, project.Env()).
		Body(map[string]any{
			"email":    "u1@SQLkite.com",
			"password": "ghanima1",
		}).
		Post(UserCreate).
		OK()

	request.ReqT(t, project.Env()).
		Body(map[string]any{
			"email":    "U1@sqlkite.COM",
			"password": "ghanima2",
		}).
		Post(UserCreate).
		ExpectValidation("email", codes.VAL_AUTH_EMAIL_IN_USE)
}

func Test_UserCreate_Project_Disabled_Aute(t *testing.T) {
	request.ReqT(t, projectAuthDisabled.Env()).
		Body(map[string]any{
			"email":    "u1@SQLkite.com",
			"password": "ghanima1",
		}).
		Post(UserCreate).
		ExpectInvalid(codes.RES_AUTH_DISABLED)
}

func mustGetProject(id string) *sqlkite.Project {
	project, err := sqlkite.Projects.Get(id)
	if err != nil {
		panic(err)
	}
	if project == nil {
		panic("Project " + id + " does not exist")
	}
	return project
}
