package tables

import (
	"github.com/valyala/fasthttp"
	"src.goblgobl.com/sqlkite"
	"src.goblgobl.com/utils/ascii"
	"src.goblgobl.com/utils/http"
)

func Delete(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
	name := ascii.Lowercase(conn.UserValue("name").(string))
	if err := env.Project.DeleteTable(env, name); err != nil {
		return nil, err
	}

	validator := env.Validator
	// possible that DeleteTable added validation errors
	if !validator.IsValid() {
		return http.Validation(validator), nil
	}

	return http.Ok(nil), nil
}
