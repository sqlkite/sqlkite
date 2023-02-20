package tables

import (
	"github.com/valyala/fasthttp"
	"src.goblgobl.com/utils/http"
	"src.sqlkite.com/sqlkite"
)

func Delete(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
	name := conn.UserValue("name").(string)
	if err := env.Project.DeleteTable(env, name); err != nil {
		return nil, err
	}

	// possible that DeleteTable added validation errors
	vc := env.VC
	if !vc.IsValid() {
		return http.Validation(vc), nil
	}

	return http.OK(nil), nil
}
