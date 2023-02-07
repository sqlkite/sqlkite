package diagnostics

import (
	"testing"

	"src.goblgobl.com/tests/assert"
	"src.goblgobl.com/tests/request"
	"src.goblgobl.com/utils/log"
	_ "src.sqlkite.com/sqlkite/tests"
)

func Test_Ping_Ok(t *testing.T) {
	conn := request.Req(t).Conn()
	res, err := Ping(conn)
	assert.Nil(t, err)

	res.Write(conn, log.Noop{})
	body := request.Res(t, conn).OK()
	assert.Equal(t, body.Body, `{"ok":true}`)
}
