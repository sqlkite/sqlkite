package auth

import (
	"testing"
	"time"

	"src.goblgobl.com/tests/assert"
	"src.goblgobl.com/tests/request"
	"src.goblgobl.com/utils"
	"src.sqlkite.com/sqlkite"
	"src.sqlkite.com/sqlkite/codes"
	"src.sqlkite.com/sqlkite/tests"
)

var (
	standardProject *sqlkite.Project
)

func init() {
	standardProject, _ = sqlkite.Projects.Get(tests.Factory.StandardId)
}

func Test_SessionCreate_InvalidBody(t *testing.T) {
	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body("nope").
		Post(SessionCreate).
		ExpectInvalid(utils.RES_INVALID_JSON_PAYLOAD)
}

func Test_SessionCreate_InvalidData(t *testing.T) {
	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Post(SessionCreate).
		ExpectValidation("email", utils.VAL_REQUIRED, "password", utils.VAL_REQUIRED)

	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body(map[string]any{
			"email":    4,
			"password": 5,
		}).
		Post(SessionCreate).
		ExpectValidation("email", utils.VAL_STRING_TYPE, "password", utils.VAL_STRING_TYPE)

	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body(map[string]any{
			"email":    "hello",
			"password": "12345678",
		}).
		Post(SessionCreate).
		ExpectValidation("email", utils.VAL_STRING_PATTERN, "password", codes.VAL_COMMON_PASSWORD)

	request.ReqT(t, sqlkite.BuildEnv().Env()).
		Body(map[string]any{
			"password": "1234567",
		}).
		Post(SessionCreate).
		ExpectValidation("password", utils.VAL_STRING_LENGTH)
}

func Test_SessionCreate_UnknownEmail(t *testing.T) {
	request.ReqT(t, standardProject.Env()).
		Body(map[string]any{
			"email":    "duncan@sqlkite.com",
			"password": "jessica9",
		}).
		Post(SessionCreate).
		ExpectNotFound(codes.RES_SESSION_INVALID_CREDENTIALS)
}

func Test_SessionCreate_InvalidPassword(t *testing.T) {
	request.ReqT(t, standardProject.Env()).
		Body(map[string]any{
			"email":    "teg@sqlkite.com",
			"password": "Lernaeus",
		}).
		Post(SessionCreate).
		ExpectNotFound(codes.RES_SESSION_INVALID_CREDENTIALS)

	// case sensitive password
	request.ReqT(t, standardProject.Env()).
		Body(map[string]any{
			"email":    "teg@sqlkite.com",
			"password": "roxbrough",
		}).
		Post(SessionCreate).
		ExpectNotFound(codes.RES_SESSION_INVALID_CREDENTIALS)
}

func Test_SessionCreate_Success_No_SessionTTL(t *testing.T) {
	project := mustGetProject(tests.Factory.DynamicId())

	userId := tests.Generator.UUID()
	tests.Factory.User.Insert(project, "id", userId, "email", "teg@sqlkite.com", "password", "Roxbrough")

	res := request.ReqT(t, project.Env()).
		Body(map[string]any{
			"email":    "teg@sqlkite.com",
			"password": "Roxbrough",
		}).
		Post(SessionCreate).
		OK().JSON()

	assert.Nil(t, res["role"])
	assert.Equal(t, res.String("user_id"), userId)

	row := tests.Row(project, "select * from sqlkite_sessions where id = ?1", res.String("id"))
	assert.Nil(t, row["role"])
	assert.Equal(t, row.String("user_id"), userId)
	assert.Nowish(t, row.Time("created"))
	assert.Timeish(t, row.Time("expires"), time.Now().Add(time.Duration(43830)*time.Hour))
}

func Test_SessionCreate_Success_Project_SessionTTL(t *testing.T) {
	userId := tests.Generator.UUID()
	tests.Factory.User.Insert(projectAuthSpecial, "id", userId, "email", "teg@sqlkite.com", "password", "Roxbrough")

	res := request.ReqT(t, projectAuthSpecial.Env()).
		Body(map[string]any{
			"email":    "teg@sqlkite.com",
			"password": "Roxbrough",
		}).
		Post(SessionCreate).
		OK().JSON()

	assert.Nil(t, res["role"])
	assert.Equal(t, res.String("user_id"), userId)

	row := tests.Row(projectAuthSpecial, "select * from sqlkite_sessions where id = ?1", res.String("id"))
	assert.Nil(t, row["role"])
	assert.Equal(t, row.String("user_id"), userId)
	assert.Nowish(t, row.Time("created"))
	assert.Timeish(t, row.Time("expires"), time.Now().Add(time.Duration(30)*time.Minute))
}

func Test_SessionCreate_Project_Disabled_Aute(t *testing.T) {
	request.ReqT(t, projectAuthDisabled.Env()).
		Body(map[string]any{
			"email":    "u1@SQLkite.com",
			"password": "ghanima1",
		}).
		Post(SessionCreate).
		ExpectInvalid(codes.RES_AUTH_DISABLED)
}
