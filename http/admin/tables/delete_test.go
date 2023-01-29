package tables

import (
	"testing"

	"src.goblgobl.com/sqlkite"
	"src.goblgobl.com/sqlkite/tests"
	"src.goblgobl.com/tests/assert"
	"src.goblgobl.com/tests/request"
)

func Test_Delete_UnknownTable(t *testing.T) {
	request.ReqT(t, sqlkite.BuildEnv().Env()).
		UserValue("name", "test1").
		Delete(Delete).
		ExpectValidation("", 302_033)
}

func Test_Delete_Success(t *testing.T) {
	id := tests.Factory.DynamicId()
	project, _ := sqlkite.Projects.Get(id)
	request.ReqT(t, project.Env()).
		Body(map[string]any{
			"name": "Test_Delete_Success",
			"columns": []any{
				map[string]any{"name": "c1", "type": "text", "nullable": true},
			},
		}).
		Post(Create).
		OK()
	project, _ = sqlkite.Projects.Get(id)
	_, exists := project.Table("test_delete_success")
	assert.True(t, exists)

	request.ReqT(t, project.Env()).
		UserValue("name", "Test_DELETE_Success").
		Delete(Delete).
		OK()
	project, _ = sqlkite.Projects.Get(id)
	_, exists = project.Table("test_delete_success")
	assert.False(t, exists)

}
