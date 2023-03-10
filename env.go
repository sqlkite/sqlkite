package sqlkite

/*
The environment of a single request. Always tied to a project.
Loaded via the EnvHandler middleware.
*/

import (
	"encoding/json"
	"errors"

	"github.com/valyala/fasthttp"
	"src.goblgobl.com/sqlite"
	"src.goblgobl.com/utils/http"
	"src.goblgobl.com/utils/log"
	"src.goblgobl.com/utils/uuid"
	"src.goblgobl.com/utils/validation"
	"src.sqlkite.com/sqlkite/codes"
)

var (
	sqliteErrorLogData = log.NewField().
				Int("code", codes.RES_DATABASE_ERROR).
				Int("status", 500).
				Finalize()

	CLEAR_SQLKITE_USERS = []byte(sqlite.Terminate("update sqlkite_user set user_id = '', role = ''"))
)

type Env struct {
	// Reference to the project.
	// This can be null, but only in very specific cases. Specifically, some high
	// level requests aren't made within a project-context, such as when we create a
	// project.

	Project *Project

	// can most certainly be nil
	User *User

	// Anything logged with this logger will automatically have the pid (project id)
	// and rid (request id) fields
	Logger log.Logger

	// Validation context
	VC *validation.Context[*Env]

	// The logger that's used to generate the HTTP request log. The Log() method
	// should not be called on this (it'll automatically be called at the end
	// of the request)
	requestLogger log.Logger

	// Every time we get an env, we assign it a RequestId. This is essentially
	// a per-project incrementing integer. It can wrap and we can have duplicates
	// but generally, over a reasonable window, it should be unique per project.
	// Mostly just used with the logger, but exposed in case we need it.
	requestId string
}

func NewEnv(p *Project, requestId string) *Env {
	logger := log.Checkout()
	if p != nil {
		logger.Field(p.logField)
	}
	logger.String("rid", requestId).MultiUse()

	env := &Env{
		Project:   p,
		Logger:    logger,
		requestId: requestId,
	}
	env.VC = validationContexts.Checkout(env)
	return env
}

func (e *Env) WithDB(cb func(sqlite.Conn) error) error {
	user := e.User
	project := e.Project
	if user == nil {
		return project.WithDB(cb)
	}

	pool := project.dbPool
	conn := pool.Checkout()

	if err := conn.Exec("insert or replace into sqlkite_user (id, user_id, role) values (0, ?1, ?2)", user.Id, user.Role); err != nil {
		e.Error("Env.WithDB.upsert").String("uid", user.Id).String("role", user.Role).Err(err).Log()
		return err
	}

	defer func() {
		if err := conn.ExecTerminated(CLEAR_SQLKITE_USERS); err != nil {
			e.Error("Env.WithDB.clear").Err(err).Log()
		}
		pool.Release(conn)
	}()

	return cb(conn)
}

func (e *Env) RequestId() string {
	return e.requestId
}

func (e *Env) Info(ctx string) log.Logger {
	return e.Logger.Info(ctx)
}

func (e *Env) Warn(ctx string) log.Logger {
	return e.Logger.Warn(ctx)
}

func (e *Env) Error(ctx string) log.Logger {
	return e.Logger.Error(ctx)
}

func (e *Env) Fatal(ctx string) log.Logger {
	return e.Logger.Fatal(ctx)
}

func (e *Env) Request(route string) log.Logger {
	logger := log.Checkout()
	if p := e.Project; p != nil {
		logger.Field(p.logField)
	}
	logger.String("rid", e.requestId).Request(route)
	if user := e.User; user != nil {
		logger.String("uid", user.Id)
	}

	e.requestLogger = logger
	return logger
}

func (e *Env) Release() {
	e.VC.Release()
	e.Logger.Release()
}

// In a perfect world, we'd have distinct error codes and messages for
// every possible error. And we'd be able to fully validate all the inputs
// precisely (like, you can't use the greater than operator on a text column).
// Unfortunately, that's not something we can reasonably do in all cases and
// sqlite itself gives pretty generic error codes.
// If err is an sqlite.Error, there's a reasonable chance this is a user-error.
// We still want to log the error, and we still want to return a 500, but we want
// this to be a project-level error, not a system-level error.
// (Ideally, we should have 0 system-level errors, but project level errors are
// outside of our control so we don't want to log the two the same way)
func (e *Env) ServerError(err error, conn *fasthttp.RequestCtx) http.Response {
	var sqliteErr sqlite.Error
	if !errors.As(err, &sqliteErr) {
		return http.ServerError(err, e.Debug())
	}

	errorId := uuid.String()
	e.Error("sqlite_error").Err(err).String("eid", errorId).Log()

	errorMessage := "database error"
	if e.Debug() {
		errorMessage = sqliteErr.Error()
	}

	innerCode := 0
	var se *log.StructuredError
	if errors.As(err, &se) {
		innerCode = se.Code
	}

	data := struct {
		Error   string `json:"error"`
		ErrorId string `json:"error_id"`
		Code    int    `json:"code"`
		Inner   int    `json:"icode"`
	}{
		Error:   errorMessage,
		ErrorId: errorId,
		Code:    codes.RES_DATABASE_ERROR,
		Inner:   innerCode,
	}
	body, _ := json.Marshal(data)

	return http.NewErrorIdResponse(err, errorId, body, sqliteErrorLogData)
}

func (e *Env) Debug() bool {
	if project := e.Project; project != nil {
		return project.Debug
	}
	return false
}
