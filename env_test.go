package sqlkite

import (
	"errors"
	"strings"
	"testing"

	"src.goblgobl.com/sqlite"
	"src.goblgobl.com/tests/assert"
	"src.goblgobl.com/tests/request"
	"src.goblgobl.com/utils/log"
)

func Test_Env_Request(t *testing.T) {
	// no project, no user
	env := NewEnv(nil, "req1")
	assertLogger(t, env.Request("test-route1"), "rid", "req1", "_l", "req", "_c", "test-route1")
	env.Release()

	// project, no user
	project := &Project{logField: log.NewField().Int("over", 9000).Finalize()}
	env = NewEnv(project, "req2")
	assertLogger(t, env.Request("test-route2"), "rid", "req2", "_l", "req", "_c", "test-route2", "over", "9000")
	env.Release()

	// project, user
	env = NewEnv(project, "req3")
	env.User = &User{Id: "xx1"}
	assertLogger(t, env.Request("test-route3"), "rid", "req3", "_l", "req", "_c", "test-route3", "over", "9000", "uid", "xx1")
	env.Release()
}

func Test_Env_ServerError_NotSqlite(t *testing.T) {
	env := NewEnv(nil, "")
	res := env.ServerError(errors.New("hello"), nil)
	json := request.Response(t, res).ExpectStatus(500).ExpectCode(2001).Json
	assert.Equal(t, json.String("error"), "internal server error")
}

func Test_Env_ServerError_Sqlite_NoDebug(t *testing.T) {
	env := NewEnv(&Project{}, "")
	res := env.ServerError(sqlite.Error{Code: 9999999, Message: "db locked"}, nil)
	json := request.Response(t, res).ExpectStatus(500).ExpectCode(302005).Json
	assert.Equal(t, json.String("error"), "database error")
}

func Test_Env_ServerError_Sqlite_Debug(t *testing.T) {
	env := NewEnv(&Project{Debug: true}, "")
	res := env.ServerError(sqlite.Error{Code: 9999999, Message: "db locked"}, nil)
	json := request.Response(t, res).ExpectStatus(500).ExpectCode(302005).Json
	assert.Equal(t, json.String("error"), "sqlite: db locked (code: 9999999)")
}

func assertLogger(t *testing.T, logger log.Logger, expected ...string) {
	t.Helper()

	out := new(strings.Builder)
	logger.LogTo(out)

	data := log.KvParse(out.String())
	for i := 0; i < len(expected); i += 2 {
		assert.Equal(t, data[expected[i]], expected[i+1])
	}
}
