package sqlkite

/*
The environment of a single request. Always tied to a project.
Loaded via the EnvHandler middleware.
*/

import (
	"encoding/json"
	"errors"

	"src.goblgobl.com/sqlite"
	"src.goblgobl.com/sqlkite/codes"
	"src.goblgobl.com/utils/http"
	"src.goblgobl.com/utils/log"
	"src.goblgobl.com/utils/uuid"
	"src.goblgobl.com/utils/validation"
)

var (
	sqliteErrorLogData = log.NewField().
		Int("code", codes.RES_DATABASE_ERROR).
		Int("status", 500).
		Finalize()
)

type Env struct {
	// Every time we get an env, we assign it a RequestId. This is essentially
	// a per-project incrementing integer. It can wrap and we can have duplicates
	// but generally, over a reasonable window, it should be unique per project.
	// Mostly just used with the logger, but exposed in case we need it.
	requestId string

	// Reference to the project.
	// This can be null, but only in very specific cases. Specifically, some high
	// level requests aren't made within a project-context, such as when we create a
	// project.

	Project *Project

	// Anything logged with this logger will automatically have the pid (project id)
	// and rid (request id) fields
	Logger log.Logger

	// The logger that's used to generate the HTTP request log. The Log() method
	// should not be called on this (it'll automatically be called at the end
	// of the request)
	requestLogger log.Logger

	// records validation errors
	Validator *validation.Result

	// can most certainly be nil
	User *User
}

func NewEnv(p *Project, requestId string) *Env {
	logger := log.Checkout()
	if p != nil {
		logger.Field(p.logField)
	}
	logger.String("rid", requestId).MultiUse()

	return &Env{
		Project:   p,
		Logger:    logger,
		requestId: requestId,
		Validator: validation.Checkout(),
	}
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
	e.Logger.Release()
	e.Validator.Release()
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
func (e *Env) ServerError(err error) http.Response {
	var sqliteErr sqlite.Error
	if !errors.As(err, &sqliteErr) {
		return http.ServerError(err)
	}

	errorId := uuid.String()
	e.Error("sqlite_error").Err(err).String("eid", errorId).Log()

	data := struct {
		Code    int    `json:"code"`
		Error   string `json:"error"`
		ErrorId string `json:"error_id"`
	}{
		ErrorId: errorId,
		Code:    codes.RES_DATABASE_ERROR,
		Error:   "database error",
	}
	body, _ := json.Marshal(data)

	return http.ErrorIdResponse{
		Body:    body,
		ErrorId: errorId,
		LogData: sqliteErrorLogData,
	}
}
