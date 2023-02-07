package http

import (
	"errors"
	"os"
	"testing"

	"github.com/valyala/fasthttp"
	"src.goblgobl.com/tests/assert"
	"src.goblgobl.com/tests/request"
	"src.goblgobl.com/utils/http"
	"src.goblgobl.com/utils/log"
	"src.goblgobl.com/utils/uuid"
	"src.sqlkite.com/sqlkite"
	"src.sqlkite.com/sqlkite/tests"
)

var projectId string

func TestMain(m *testing.M) {
	projectId = tests.Factory.Project.Insert().String("id")
	if err := sqlkite.CreateDB(projectId); err != nil {
		panic(err)
	}

	code := m.Run()
	tests.RemoveTempDBs()
	os.Exit(code)
}

func Test_Server_Env_Project_FromHeader(t *testing.T) {
	conn := request.Req(t).Conn()
	http.Handler("", createEnvLoader(loadUserFromHeader, loadProjectIdFromHeader), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
		assert.Fail(t, "next should not be called")
		return nil, nil
	})(conn)
	request.Res(t, conn).ExpectInvalid(302_002)

	// invalid project id
	conn = request.Req(t).ProjectId("nope").Conn()
	http.Handler("", createEnvLoader(loadUserFromHeader, loadProjectIdFromHeader), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
		assert.Fail(t, "next should not be called")
		return nil, nil
	})(conn)
	request.Res(t, conn).ExpectInvalid(302_004)

	// project id not found
	conn = request.Req(t).ProjectId(uuid.String()).Conn()
	http.Handler("", createEnvLoader(loadUserFromHeader, loadProjectIdFromHeader), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
		assert.Fail(t, "next should not be called")
		return nil, nil
	})(conn)
	request.Res(t, conn).ExpectInvalid(302_004)
}

func Test_Server_Env_Project_FromSubdomain(t *testing.T) {
	conn := request.Req(t).Host("single").Conn()
	http.Handler("", createEnvLoader(loadUserFromHeader, loadProjectIdFromSubdomain), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
		assert.Fail(t, "next should not be called")
		return nil, nil
	})(conn)
	request.Res(t, conn).ExpectInvalid(302_003)

	conn = request.Req(t).Host("sqlkite.com").Conn()
	http.Handler("", createEnvLoader(loadUserFromHeader, loadProjectIdFromSubdomain), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
		assert.Fail(t, "next should not be called")
		return nil, nil
	})(conn)
	request.Res(t, conn).ExpectInvalid(302_004)

	conn = request.Req(t).Host("n1.sqlkite.com").Conn()
	http.Handler("", createEnvLoader(loadUserFromHeader, loadProjectIdFromSubdomain), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
		assert.Fail(t, "next should not be called")
		return nil, nil
	})(conn)
	request.Res(t, conn).ExpectInvalid(302_004)
}

func Test_Server_Env_Unknown_Project(t *testing.T) {
	conn := request.Req(t).ProjectId("6429C13A-DBB2-4FF2-ADDA-571C601B91E6").Conn()
	http.Handler("", createEnvLoader(loadUserFromHeader, loadProjectIdFromHeader), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
		assert.Fail(t, "next should not be called")
		return nil, nil
	})(conn)
	request.Res(t, conn).ExpectInvalid(302_004)
}

func Test_Server_Env_CallsHandlerWithProject(t *testing.T) {
	conn := request.Req(t).ProjectId(projectId).Conn()
	http.Handler("", createEnvLoader(loadUserFromHeader, loadProjectIdFromHeader), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
		assert.Equal(t, env.Project.Id, projectId)
		return http.Ok(map[string]int{"over": 9000}), nil
	})(conn)

	res := request.Res(t, conn).OK()
	assert.Equal(t, res.Json.Int("over"), 9000)
}

func Test_Server_Env_RequestId(t *testing.T) {
	conn := request.Req(t).ProjectId(projectId).Conn()

	var id1, id2 string
	http.Handler("", createEnvLoader(loadUserFromHeader, loadProjectIdFromHeader), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
		id1 = env.RequestId()
		return http.Ok(nil), nil
	})(conn)

	http.Handler("", createEnvLoader(loadUserFromHeader, loadProjectIdFromHeader), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
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
		http.Handler("test-route", createEnvLoader(loadUserFromHeader, loadProjectIdFromHeader), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
			requestId = env.RequestId()
			return http.StaticNotFound(9001), nil
		})(conn)
	})

	reqLog := log.KvParse(logged)
	assert.Equal(t, reqLog["_l"], "req")
	assert.Equal(t, reqLog["_code"], "9001")
	assert.Equal(t, reqLog["_c"], "test-route")
	assert.Equal(t, reqLog["pid"], projectId)
	assert.Equal(t, reqLog["rid"], requestId)
	assert.Equal(t, reqLog["status"], "404")
	assert.Equal(t, reqLog["res"], "33")
}

func Test_Server_Env_LogsError(t *testing.T) {
	var requestId string
	conn := request.Req(t).ProjectId(projectId).Conn()
	logged := tests.CaptureLog(func() {
		http.Handler("test2", createEnvLoader(loadUserFromHeader, loadProjectIdFromHeader), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
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
	assert.Equal(t, reqLog["_code"], "2001")
	assert.Equal(t, reqLog["_l"], "req")
	assert.Equal(t, reqLog["_c"], "test2")
	assert.Equal(t, reqLog["_err"], `"Not Over 9000!"`)
	assert.Equal(t, reqLog["pid"], projectId)
	assert.Equal(t, reqLog["rid"], requestId)
	assert.Equal(t, reqLog["status"], "500")
	assert.Equal(t, reqLog["res"], "95")
	assert.Equal(t, reqLog["eid"], errorId)
}

func Test_Server_SuperEnv_NoUser(t *testing.T) {
	conn := request.Req(t).Conn()
	http.Handler("", loadSuperEnv(loadUserFromHeader), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
		assert.Fail(t, "next should not be called")
		return nil, nil
	})(conn)
	request.Res(t, conn).ExpectNotAuthorized(302_006)
}

func Test_Server_SuperEnv_NonSuperRole(t *testing.T) {
	conn := request.Req(t).User("id1").Conn()
	http.Handler("", loadSuperEnv(loadUserFromHeader), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
		assert.Fail(t, "next should not be called")
		return nil, nil
	})(conn)
	request.Res(t, conn).ExpectForbidden(302_007)

	conn = request.Req(t).User("id1", "admin").Conn()
	http.Handler("", loadSuperEnv(loadUserFromHeader), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
		assert.Fail(t, "next should not be called")
		return nil, nil
	})(conn)
	request.Res(t, conn).ExpectForbidden(302_007)
}

func Test_Server_SuperEnv_Ok(t *testing.T) {
	conn := request.Req(t).User("u1", "super").Conn()

	var id1, id2 string
	http.Handler("", loadSuperEnv(loadUserFromHeader), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
		assert.Nil(t, env.Project)
		id1 = env.RequestId()
		return http.Ok(nil), nil
	})(conn)

	http.Handler("", loadSuperEnv(loadUserFromHeader), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
		assert.Nil(t, env.Project)
		id2 = env.RequestId()
		return http.Ok(nil), nil
	})(conn)

	assert.Equal(t, len(id1), 8)
	assert.Equal(t, len(id2), 8)
	assert.NotEqual(t, id1, id2)
}

func Test_Server_Nil_User_FromHeader(t *testing.T) {
	called := false
	conn := request.Req(t).ProjectId(projectId).Conn()
	http.Handler("", createEnvLoader(loadUserFromHeader, loadProjectIdFromHeader), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
		called = true
		assert.Nil(t, env.User)
		return http.Ok(nil), nil
	})(conn)
	assert.True(t, called)
}

func Test_Server_User_NoRole_FromHeader(t *testing.T) {
	called := false
	conn := request.Req(t).ProjectId(projectId).User("user1").Conn()
	http.Handler("", createEnvLoader(loadUserFromHeader, loadProjectIdFromHeader), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
		called = true
		assert.Equal(t, env.User.Id, "user1")
		assert.Equal(t, env.User.Role, "")
		return http.Ok(nil), nil
	})(conn)
	assert.True(t, called)
}

func Test_Server_User_WithRole_FromHeader(t *testing.T) {
	called := false
	conn := request.Req(t).ProjectId(projectId).User("user1", "admin").Conn()
	http.Handler("", createEnvLoader(loadUserFromHeader, loadProjectIdFromHeader), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
		called = true
		assert.Equal(t, env.User.Id, "user1")
		assert.Equal(t, env.User.Role, "admin")
		return http.Ok(nil), nil
	})(conn)
	assert.True(t, called)
}

func Test_Server_RequireRole_NoCredentials(t *testing.T) {
	conn := request.Req(t).ProjectId(projectId).Conn()
	http.Handler("", requireRole("super", createEnvLoader(loadUserFromHeader, loadProjectIdFromHeader)), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
		assert.Fail(t, "next should not be called")
		return nil, nil
	})(conn)
	request.Res(t, conn).ExpectNotAuthorized(302_006)
}

func Test_Server_RequireRole_InvalidRole(t *testing.T) {
	conn := request.Req(t).ProjectId(projectId).User("id1").Conn()
	http.Handler("", requireRole("super", createEnvLoader(loadUserFromHeader, loadProjectIdFromHeader)), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
		assert.Fail(t, "next should not be called")
		return nil, nil
	})(conn)
	request.Res(t, conn).ExpectForbidden(302_007)

	conn = request.Req(t).ProjectId(projectId).User("id1", "guest").Conn()
	http.Handler("", requireRole("super", createEnvLoader(loadUserFromHeader, loadProjectIdFromHeader)), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
		assert.Fail(t, "next should not be called")
		return nil, nil
	})(conn)
	request.Res(t, conn).ExpectForbidden(302_007)
}

func Test_Server_RequireRole_ValidRole(t *testing.T) {
	called := false
	conn := request.Req(t).ProjectId(projectId).User("id1", "super").Conn()
	http.Handler("", requireRole("super", createEnvLoader(loadUserFromHeader, loadProjectIdFromHeader)), func(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
		called = true
		return http.Ok(nil), nil
	})(conn)
	assert.True(t, called)
}
