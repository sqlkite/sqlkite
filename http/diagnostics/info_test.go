package diagnostics

import (
	"runtime"
	"testing"

	"src.goblgobl.com/tests"
	"src.goblgobl.com/tests/assert"
	"src.goblgobl.com/tests/request"
	"src.goblgobl.com/utils/log"
	_ "src.sqlkite.com/sqlkite/tests"
)

func Test_Info_Ok(t *testing.T) {
	conn := request.Req(t).Conn()
	res, err := Info(conn)
	assert.Nil(t, err)

	res.Write(conn, log.Noop{})
	body := request.Res(t, conn).OK().JSON()
	assert.Equal(t, body.String("commit"), commit)
	assert.Equal(t, body.String("go"), runtime.Version())
	assert.Equal(t, body.Object("super").String("type"), tests.StorageType())
}
