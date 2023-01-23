package sqlkite

import (
	"testing"
	"time"

	"src.goblgobl.com/sqlkite/data"
	"src.goblgobl.com/sqlkite/tests"
	"src.goblgobl.com/tests/assert"
)

func Test_Project_NextRequestId(t *testing.T) {
	seen := make(map[string]struct{}, 60)

	p := Project{requestId: 1}
	for i := 0; i < 20; i++ {
		seen[p.NextRequestId()] = struct{}{}
	}

	p = Project{requestId: 100}
	for i := 0; i < 20; i++ {
		seen[p.NextRequestId()] = struct{}{}
	}

	Config.InstanceId += 1
	p = Project{requestId: 1}
	for i := 0; i < 20; i++ {
		seen[p.NextRequestId()] = struct{}{}
	}

	assert.Equal(t, len(seen), 60)
}

func Test_Project_Env(t *testing.T) {
	p := Project{requestId: 1}
	env := p.Env()
	assert.Equal(t, env.Project, &p)
	assert.Equal(t, env.RequestId(), "AAAAAAQB")
}

func Test_Projects_Get_Unknown(t *testing.T) {
	p, err := Projects.Get("6429C13A-DBB2-4FF2-ADDA-571C601B91E6")
	assert.Nil(t, p)
	assert.Nil(t, err)
}

func Test_Projects_Get_Known(t *testing.T) {
	id := tests.Factory.StandardId
	p, err := Projects.Get(id)
	assert.Nil(t, err)
	assert.Equal(t, p.Id, id)
	assert.Nowish(t, time.Unix(int64(p.requestId), 0))
	assert.Equal(t, string(p.logField.KV()), "pid="+id)
}

func Test_Project_CreateTable(t *testing.T) {
	project := dynamicProject(t)
	err := project.CreateTable(project.Env(), data.Table{
		Name: "tab1",
		Columns: []data.Column{
			data.BuildColumn().Name("c1").Type("text").Nullable().Column(),
			data.BuildColumn().Name("c2").Type("int").Nullable().Column(),
			data.BuildColumn().Name("c3").Type("real").Nullable().Column(),
			data.BuildColumn().Name("c4").Type("blob").Nullable().Column(),
		},
	})
	assert.Nil(t, err)

	err = project.CreateTable(project.Env(), data.Table{
		Name: "tab2",
		Columns: []data.Column{
			data.BuildColumn().Name("c1").Type("text").NotNullable().Default("def-1").Column(),
			data.BuildColumn().Name("c2").Type("int").NotNullable().Default(9001).Column(),
			data.BuildColumn().Name("c3").Type("real").NotNullable().Default(8999.9).Column(),
			data.BuildColumn().Name("c4").Type("blob").NotNullable().Default([]byte("d9")).Column(),
		},
	})
	assert.Nil(t, err)

	project, err = Projects.Get(project.Id)
	assert.Nil(t, err)

	table, ok := project.Table("tab1")
	assert.True(t, ok)

	// tab1 (nullable, no default)
	assert.Equal(t, table.Name, "tab1")
	assert.Equal(t, len(table.Columns), 4)

	assert.Equal(t, table.Columns[0].Name, "c1")
	assert.Nil(t, table.Columns[0].Default)
	assert.Equal(t, table.Columns[0].Nullable, true)
	assert.Equal(t, table.Columns[0].Type, data.COLUMN_TYPE_TEXT)

	assert.Equal(t, table.Columns[1].Name, "c2")
	assert.Nil(t, table.Columns[1].Default)
	assert.Equal(t, table.Columns[1].Nullable, true)
	assert.Equal(t, table.Columns[1].Type, data.COLUMN_TYPE_INT)

	assert.Equal(t, table.Columns[2].Name, "c3")
	assert.Nil(t, table.Columns[2].Default)
	assert.Equal(t, table.Columns[2].Nullable, true)
	assert.Equal(t, table.Columns[2].Type, data.COLUMN_TYPE_REAL)

	assert.Equal(t, table.Columns[3].Name, "c4")
	assert.Nil(t, table.Columns[3].Default)
	assert.Equal(t, table.Columns[3].Nullable, true)
	assert.Equal(t, table.Columns[3].Type, data.COLUMN_TYPE_BLOB)

	table, ok = project.Table("tab2")
	assert.True(t, ok)

	// tab2 (not null, with defaults)
	assert.Equal(t, table.Name, "tab2")
	assert.Equal(t, len(table.Columns), 4)

	assert.Equal(t, table.Columns[0].Name, "c1")
	assert.Equal(t, table.Columns[0].Nullable, false)
	assert.Equal(t, table.Columns[0].Default.(string), "def-1")
	assert.Equal(t, table.Columns[0].Type, data.COLUMN_TYPE_TEXT)

	assert.Equal(t, table.Columns[1].Name, "c2")
	assert.Equal(t, table.Columns[1].Nullable, false)
	assert.Equal(t, table.Columns[1].Default.(float64), 9001)
	assert.Equal(t, table.Columns[1].Type, data.COLUMN_TYPE_INT)

	assert.Equal(t, table.Columns[2].Name, "c3")
	assert.Equal(t, table.Columns[2].Nullable, false)
	assert.Equal(t, table.Columns[2].Default.(float64), 8999.9)
	assert.Equal(t, table.Columns[2].Type, data.COLUMN_TYPE_REAL)

	assert.Equal(t, table.Columns[3].Name, "c4")
	assert.Equal(t, table.Columns[3].Nullable, false)
	assert.Equal(t, string(table.Columns[3].Default.([]byte)), "d9")
	assert.Equal(t, table.Columns[3].Type, data.COLUMN_TYPE_BLOB)
}

func dynamicProject(t *testing.T) *Project {
	t.Helper()
	p, err := Projects.Get(tests.Factory.DynamicId())
	assert.Nil(t, err)
	return p
}
