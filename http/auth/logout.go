package auth

import (
	"github.com/valyala/fasthttp"
	"src.goblgobl.com/utils/http"
	"src.sqlkite.com/sqlkite"
)

func Logout(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
	// err = env.WithDB(func(conn sqlite.Conn) error {
	// 	return conn.Exec(`delete from sqlkite_sessions where id = ?1`, email)
	// })
	return nil, nil
}
