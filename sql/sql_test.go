package sql

import (
	"regexp"
	"testing"

	"src.goblgobl.com/tests/assert"
	"src.goblgobl.com/utils/buffer"
	"src.sqlkite.com/sqlkite/tests"
)

func Test_DataField_Write(t *testing.T) {
	tests.AssertSQL(t, DataField{
		Name: "full_name",
	}, "full_name")

	tests.AssertSQL(t, DataField{
		Table: "t1",
		Name:  "full_name",
	}, "t1.full_name")

	tests.AssertSQL(t, DataField{
		Name:  "full_name",
		Alias: &Alias{"name"},
	}, "full_name as name")

	tests.AssertSQL(t, DataField{
		Table: "t1",
		Name:  "full_name",
		Alias: &Alias{"name"},
	}, "t1.full_name as name")
}

func Test_DataField_WriteAsJsonObject(t *testing.T) {
	assertColumnJson(t, DataField{
		Name: "full_name",
	}, "'full_name', full_name")

	assertColumnJson(t, DataField{
		Table: "t1",
		Name:  "full_name",
	}, "'full_name', t1.full_name")

	assertColumnJson(t, DataField{
		Name:  "full_name",
		Alias: &Alias{"name"},
	}, "'name', full_name")

	assertColumnJson(t, DataField{
		Table: "t1",
		Name:  "full_name",
		Alias: &Alias{"name"},
	}, "'name', t1.full_name")
}

var sqlNormalizePattern = regexp.MustCompile("\\s+")

func assertColumnJson(t *testing.T, df DataField, expectedSQL string) {
	t.Helper()
	buf := buffer.New(256, 256)
	df.WriteAsJsonObject(buf)
	assert.Equal(t, buf.MustString(), expectedSQL)
}
