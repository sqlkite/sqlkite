package http

import (
	"errors"
	"os"
	"testing"

	"github.com/valyala/fasthttp"
	"src.goblgobl.com/sqlkite"
	"src.goblgobl.com/sqlkite/tests"
	"src.goblgobl.com/tests/assert"
	"src.goblgobl.com/tests/request"
	"src.goblgobl.com/utils/http"
	"src.goblgobl.com/utils/log"
	"src.goblgobl.com/utils/uuid"
)

var projectId string

func TestMain(m *testing.M) {
	projectId = tests.Factory.Project.Insert().String("id")
	if err := sqlkite.CreateDB(projectId); err != nil {
		panic(err)
	}
	defer tests.RemoveTempDBs()

	code := m.Run()
	os.Exit(code)
}

func Test_Server_Env_Project_FromHeader(t *testing.T) {
	conn := request.Req(t).Conn()
	http.Handler("", createEnvLoader(nil, loadProjectIdFromHeader), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
		assert.Fail(t, "next should not be called")
		return nil, nil
	})(conn)
	request.Res(t, conn).ExpectInvalid(302_002)

	// invalid project id
	conn = request.Req(t).ProjectId("nope").Conn()
	http.Handler("", createEnvLoader(nil, loadProjectIdFromHeader), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
		assert.Fail(t, "next should not be called")
		return nil, nil
	})(conn)
	request.Res(t, conn).ExpectInvalid(302_004)

	// project id not found
	conn = request.Req(t).ProjectId(uuid.String()).Conn()
	http.Handler("", createEnvLoader(nil, loadProjectIdFromHeader), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
		assert.Fail(t, "next should not be called")
		return nil, nil
	})(conn)
	request.Res(t, conn).ExpectInvalid(302_004)
}

func Test_Server_Env_Project_FromSubdomain(t *testing.T) {
	conn := request.Req(t).Host("single").Conn()
	http.Handler("", createEnvLoader(nil, loadProjectIdFromSubdomain), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
		assert.Fail(t, "next should not be called")
		return nil, nil
	})(conn)
	request.Res(t, conn).ExpectInvalid(302_003)

	conn = request.Req(t).Host("sqlkite.com").Conn()
	http.Handler("", createEnvLoader(nil, loadProjectIdFromSubdomain), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
		assert.Fail(t, "next should not be called")
		return nil, nil
	})(conn)
	request.Res(t, conn).ExpectInvalid(302_004)

	conn = request.Req(t).Host("n1.sqlkite.com").Conn()
	http.Handler("", createEnvLoader(nil, loadProjectIdFromSubdomain), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
		assert.Fail(t, "next should not be called")
		return nil, nil
	})(conn)
	request.Res(t, conn).ExpectInvalid(302_004)
}

func Test_Server_Env_Unknown_Project(t *testing.T) {
	conn := request.Req(t).ProjectId("6429C13A-DBB2-4FF2-ADDA-571C601B91E6").Conn()
	http.Handler("", createEnvLoader(nil, loadProjectIdFromHeader), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
		assert.Fail(t, "next should not be called")
		return nil, nil
	})(conn)
	request.Res(t, conn).ExpectInvalid(302_004)
}

func Test_Server_Env_CallsHandlerWithProject(t *testing.T) {
	conn := request.Req(t).ProjectId(projectId).Conn()
	http.Handler("", createEnvLoader(nil, loadProjectIdFromHeader), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
		assert.Equal(t, env.Project.Id, projectId)
		return http.Ok(map[string]int{"over": 9000}), nil
	})(conn)

	res := request.Res(t, conn).OK()
	assert.Equal(t, res.Json.Int("over"), 9000)
}

func Test_Server_Env_RequestId(t *testing.T) {
	conn := request.Req(t).ProjectId(projectId).Conn()

	var id1, id2 string
	http.Handler("", createEnvLoader(nil, loadProjectIdFromHeader), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
		id1 = env.RequestId()
		return http.Ok(nil), nil
	})(conn)

	http.Handler("", createEnvLoader(nil, loadProjectIdFromHeader), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
		id2 = env.RequestId()
		return http.Ok(nil), nil
	})(conn)

	assert.Equal(t, len(id1), 8)
	assert.Equal(t, len(id2), 8)
	assert.NotEqual(t, id1, id2)
}

func Test_Server_Env_LogsResponse(t *testing.T) {
	var requestId string
	conn := request.Req(t).ProjectId(projectId).Conn()

	logged := tests.CaptureLog(func() {
		http.Handler("test-route", createEnvLoader(nil, loadProjectIdFromHeader), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
			requestId = env.RequestId()
			return http.StaticNotFound(9001), nil
		})(conn)
	})

	reqLog := log.KvParse(logged)
	assert.Equal(t, reqLog["pid"], projectId)
	assert.Equal(t, reqLog["rid"], requestId)
	assert.Equal(t, reqLog["l"], "req")
	assert.Equal(t, reqLog["status"], "404")
	assert.Equal(t, reqLog["res"], "33")
	assert.Equal(t, reqLog["code"], "9001")
	assert.Equal(t, reqLog["c"], "test-route")
}

func Test_Server_Env_LogsError(t *testing.T) {
	var requestId string
	conn := request.Req(t).ProjectId(projectId).Conn()
	logged := tests.CaptureLog(func() {
		http.Handler("test2", createEnvLoader(nil, loadProjectIdFromHeader), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
			requestId = env.RequestId()
			return nil, errors.New("Not Over 9000!")
		})(conn)
	})

	res := request.Res(t, conn).ExpectCode(2001)
	assert.Equal(t, res.Status, 500)

	errorId := res.Headers["Error-Id"]

	assert.Equal(t, len(errorId), 36)
	assert.Equal(t, res.Json.String("error_id"), errorId)

	reqLog := log.KvParse(logged)
	assert.Equal(t, reqLog["pid"], projectId)
	assert.Equal(t, reqLog["rid"], requestId)
	assert.Equal(t, reqLog["l"], "req")
	assert.Equal(t, reqLog["status"], "500")
	assert.Equal(t, reqLog["res"], "95")
	assert.Equal(t, reqLog["code"], "2001")
	assert.Equal(t, reqLog["c"], "test2")
	assert.Equal(t, reqLog["eid"], errorId)
	assert.Equal(t, reqLog["err"], `"Not Over 9000!"`)
}

func Test_Server_SuperEnv(t *testing.T) {
	conn := request.Req(t).Conn()

	var id1, id2 string
	http.Handler("", loadSuperEnv, func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
		assert.Nil(t, env.Project)
		id1 = env.RequestId()
		return http.Ok(nil), nil
	})(conn)

	http.Handler("", loadSuperEnv, func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
		assert.Nil(t, env.Project)
		id2 = env.RequestId()
		return http.Ok(nil), nil
	})(conn)

	assert.Equal(t, len(id1), 8)
	assert.Equal(t, len(id2), 8)
	assert.NotEqual(t, id1, id2)
}
