package diagnostics

import (
	"github.com/valyala/fasthttp"
	"src.goblgobl.com/utils/http"
	"src.sqlkite.com/sqlkite/super"
)

func Ping(conn *fasthttp.RequestCtx) (http.Response, error) {
	if err := super.DB.Ping(); err != nil {
		return nil, err
	}
	return http.OKBytes([]byte(`{"ok":true}`)), nil
}
