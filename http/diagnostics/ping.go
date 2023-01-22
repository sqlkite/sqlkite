package diagnostics

import (
	"fmt"

	"github.com/valyala/fasthttp"
	"src.goblgobl.com/sqlkite/super"
	"src.goblgobl.com/utils/http"
)

func Ping(conn *fasthttp.RequestCtx) (http.Response, error) {
	if err := super.DB.Ping(); err != nil {
		return nil, fmt.Errorf("ping store - %w", err)
	}

	return http.OkBytes([]byte(`{"ok":true}`)), nil
}
