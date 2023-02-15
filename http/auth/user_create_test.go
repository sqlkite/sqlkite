package auth

import (
	"testing"

	"src.goblgobl.com/tests/request"
	"src.goblgobl.com/utils"
	"src.sqlkite.com/sqlkite"
	"src.sqlkite.com/sqlkite/codes"
)

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
		ExpectValidation("password", utils.VAL_STRING_LEN)
}
