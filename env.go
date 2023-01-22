package sqlkite

/*
The environment of a single request. Always tied to a project.
Loaded via the EnvHandler middleware.
*/

import (
	"src.goblgobl.com/utils/log"
	"src.goblgobl.com/utils/validation"
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

	e.requestLogger = logger
	return logger
}

func (e *Env) Release() {
	e.Logger.Release()
	e.Validator.Release()
}
