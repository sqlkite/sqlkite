package sql

import (
	"regexp"
	"testing"

	"src.goblgobl.com/tests/assert"
	"src.goblgobl.com/utils/buffer"
)

func Test_DataField_Write(t *testing.T) {
	assertSQL(t, DataField{
		Name: "full_name",
	}, "full_name")

	assertSQL(t, DataField{
		Table: "t1",
		Name:  "full_name",
	}, "t1.full_name")

	assertSQL(t, DataField{
		Name:  "full_name",
		Alias: &Alias{"name"},
	}, "full_name as name")

	assertSQL(t, DataField{
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

func assertSQL(t *testing.T, part Part, expectedSQL string) {
	t.Helper()
	buf := buffer.New(512, 512)
	part.Write(buf)
	actual := sqlNormalizePattern.ReplaceAllString(buf.MustString(), " ")
	assert.Equal(t, actual, expectedSQL)
}

func assertColumnJson(t *testing.T, df DataField, expectedSQL string) {
	t.Helper()
	buf := buffer.New(256, 256)
	df.WriteAsJsonObject(buf)
	assert.Equal(t, buf.MustString(), expectedSQL)
}
