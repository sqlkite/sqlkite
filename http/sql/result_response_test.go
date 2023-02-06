package sql

import (
	"testing"

	"github.com/valyala/fasthttp"
	"src.goblgobl.com/sqlkite"
	"src.goblgobl.com/tests/assert"
	"src.goblgobl.com/utils/buffer"
	"src.goblgobl.com/utils/log"
)

func Test_ResultResponse_WithData(t *testing.T) {
	assertResultResponse(t, `{"id":1}`, `{"r":[{"id":1}]}`)
}

func Test_ResultResponse_RowCountOnly(t *testing.T) {
	assertResultResponse(t, 0, `{"affected":0}`)
	assertResultResponse(t, 99, `{"affected":99}`)
}

func assertResultResponse(t *testing.T, data any, expected string) {
	t.Helper()

	qr := new(sqlkite.QueryResult)
	if n, ok := data.(int); ok {
		qr.RowCount = n
	} else {
		d := data.(string)
		qr.RowCount = 1
		qr.Result = buffer.Containing([]byte(d), len(d))
	}

	conn := new(fasthttp.RequestCtx)
	NewResultResponse(qr).Write(conn, log.Noop{})
	assert.Equal(t, string(conn.Response.Body()), expected)
}
