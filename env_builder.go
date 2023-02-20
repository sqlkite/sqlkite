//go:build !release

// Used as a factory for tests only

package sqlkite

import (
	"src.goblgobl.com/utils/log"
	"src.goblgobl.com/utils/uuid"
)

type EnvBuilder struct {
	project *Project
	logger  log.Logger
}

func BuildEnv() *EnvBuilder {
	project := &Project{Id: uuid.String()}
	return &EnvBuilder{
		project: project,
	}
}

func (eb *EnvBuilder) ProjectId(id string) *EnvBuilder {
	eb.project.Id = id
	return eb
}

func (eb *EnvBuilder) NoProject() *EnvBuilder {
	eb.project = nil
	return eb
}

func (eb *EnvBuilder) Env() *Env {
	logger := eb.logger
	if logger == nil {
		logger = log.Noop{}
	}

	env := &Env{
		Logger:  logger,
		Project: eb.project,
	}
	env.VC = validationContexts.Checkout(env)
	return env
}
