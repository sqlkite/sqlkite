package diagnostics

import (
	_ "embed"
	"runtime"

	"github.com/valyala/fasthttp"
	"src.goblgobl.com/utils/http"
	"src.sqlkite.com/sqlkite/super"
)

//go:generate make commit.txt
//go:embed commit.txt
var commit string

func Info(conn *fasthttp.RequestCtx) (http.Response, error) {
	superInfo, err := super.DB.Info()
	if err != nil {
		return nil, err
	}

	return http.Ok(struct {
		Go     string `json:"go"`
		Commit string `json:"commit"`
		Super  any    `json:"super"`
	}{
		Commit: commit,
		Go:     runtime.Version(),
		Super:  superInfo,
	}), nil
}
