package diagnostics

import (
	_ "embed"
	"fmt"
	"runtime"

	"github.com/valyala/fasthttp"
	"src.goblgobl.com/sqlkite/super"
	"src.goblgobl.com/utils/http"
)

//go:generate make commit.txt
//go:embed commit.txt
var commit string

func Info(conn *fasthttp.RequestCtx) (http.Response, error) {
	superInfo, err := super.DB.Info()
	if err != nil {
		return nil, fmt.Errorf("super info - %w", err)
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
