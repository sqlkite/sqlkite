package sqlkite

import (
	"testing"
	"time"

	"src.goblgobl.com/sqlite"
	"src.goblgobl.com/tests/assert"
	"src.sqlkite.com/sqlkite/data"
	"src.sqlkite.com/sqlkite/sql"
	"src.sqlkite.com/sqlkite/tests"
)

func Test_NewProject(t *testing.T) {
	project, err := NewProject(&data.Project{
		Id:                   tests.Factory.StandardId,
		MaxSQLLength:         200,
		MaxResultLength:      2000,
		MaxDatabaseSize:      40930,
		MaxConcurrency:       10,
		MaxSQLParameterCount: 11,
		MaxRowCount:          12,
		MaxFromCount:         13,
		MaxSelectColumnCount: 14,
		MaxConditionCount:    15,
		MaxOrderByCount:      16,
		MaxTableCount:        17,
	}, true)

	assert.Nil(t, err)
	defer project.Shutdown()
	assert.Equal(t, project.Id, tests.Factory.StandardId)
	assert.Equal(t, project.MaxConcurrency, 10)
	assert.Equal(t, project.MaxDatabaseSize, 40930)
	assert.Equal(t, project.MaxSQLLength, 200)
	assert.Equal(t, project.MaxSQLParameterCount, 11)
	assert.Equal(t, project.MaxRowCount, 12)
	assert.Equal(t, project.MaxResultLength, 2000)
	assert.Equal(t, project.MaxFromCount, 13)
	assert.Equal(t, project.MaxSelectColumnCount, 14)
	assert.Equal(t, project.MaxConditionCount, 15)
	assert.Equal(t, project.MaxOrderByCount, 16)
	assert.Equal(t, project.MaxTableCount, 17)
}

func Test_NewProject_DBSize(t *testing.T) {
	project, err := NewProject(&data.Project{
		Id:              tests.Factory.StandardId,
		MaxDatabaseSize: 40930,
	}, true)

	assert.Nil(t, err)
	defer project.Shutdown()

	var pageSize, pageCount int
	project.WithDB(func(conn sqlite.Conn) {
		conn.Row("pragma page_size").Scan(&pageSize)
		conn.Row("pragma max_page_count").Scan(&pageCount)
	})

	assert.Equal(t, pageCount, 40930/pageSize)
}

func Test_Project_WithDBEnv(t *testing.T) {
	project := MustGetProject(tests.Factory.StandardId)
	env := project.Env()
	env.User = &User{Id: "user-1", Role: "role-a"}
	defer env.Release()

	var userId, role string
	project.WithDBEnv(env, func(conn sqlite.Conn) error {
		err := conn.Row("select sqlkite_user_id(), sqlkite_user_role()").Scan(&userId, &role)
		assert.Nil(t, err)
		return nil
	})

	assert.Equal(t, userId, "user-1")
	assert.Equal(t, role, "role-a")
}

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
	project := dynamicProject()
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

	project = MustGetProject(project.Id)
	err = project.CreateTable(project.Env(), data.Table{
		Name: "tab2",
		Columns: []data.Column{
			data.BuildColumn().Name("c1").Type("text").NotNullable().Default("def-1").Column(),
			data.BuildColumn().Name("c2").Type("int").NotNullable().Default(9001).Column(),
			data.BuildColumn().Name("c3").Type("real").NotNullable().Default(8999.9).Column(),
			data.BuildColumn().Name("c4").Type("blob").NotNullable().Default([]byte("d9")).Column(),
		},
		Access: data.TableAccess{
			Select: &data.SelectTableAccess{CTE: "select * from tab2 where public"},
		},
	})
	assert.Nil(t, err)

	project = MustGetProject(project.Id)
	table := project.Table("tab1")
	assert.NotNil(t, table)

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

	assert.Nil(t, table.Access.Select)

	table = project.Table("tab2")
	assert.NotNil(t, table)

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

	assert.Equal(t, table.Access.Select.CTE, "select * from tab2 where public")
	assert.Equal(t, table.Access.Select.Name, "sqlkite_cte_tab2")
}

func Test_Project_UpdateTable_UnknownTable(t *testing.T) {
	project := MustGetProject(tests.Factory.StandardId)
	env := project.Env()
	err := project.UpdateTable(env, sql.AlterTable{Name: "tab1"}, data.TableAccess{})
	assert.Nil(t, err)
	assert.Validation(t, env.Validator).Field("", 302_033, map[string]any{"value": "tab1"})
}

func Test_Project_UpdateTable_Success(t *testing.T) {
	id := tests.Factory.DynamicId()
	project := MustGetProject(id)
	err := project.CreateTable(project.Env(), data.Table{
		Name: "tab_update",
		Columns: []data.Column{
			data.BuildColumn().Name("c1").Type("text").Nullable().Column(),
			data.BuildColumn().Name("c2").Type("int").Nullable().Column(),
			data.BuildColumn().Name("c3").Type("real").Nullable().Column(),
			data.BuildColumn().Name("c4").Type("blob").Nullable().Column(),
		},
	})
	assert.Nil(t, err)

	project = MustGetProject(id)
	err = project.UpdateTable(project.Env(), sql.AlterTable{
		Name: "tab_update",
		Changes: []sql.AlterTableChange{
			sql.DropColumn{Name: "c2"},
			sql.RenameColumn{Name: "c1", To: "c1_b"},
			sql.AddColumn{Column: data.BuildColumn().Name("c5").Type("int").NotNullable().Column()},
			sql.DropColumn{Name: "c4"},
			sql.RenameTable{To: "tab_update_b"},
		}},
		data.TableAccess{Select: &data.SelectTableAccess{CTE: "select 1"}})
	assert.Nil(t, err)

	// Projects & tables are meant to be immutable. Changes to a project are only reflected
	// by refetching the project. Let's make sure that our update didn't change
	// the instance of the project that UpdateTable was called on
	_, exists := project.tables["tab_update_b"]
	assert.False(t, exists)
	table := project.tables["tab_update"]
	assert.Equal(t, table.Name, "tab_update")
	assert.Equal(t, len(table.Columns), 4)
	assert.Equal(t, table.Columns[0].Name, "c1")
	assert.Equal(t, table.Columns[1].Name, "c2")
	assert.Equal(t, table.Columns[2].Name, "c3")
	assert.Equal(t, table.Columns[3].Name, "c4")

	// ok, now let's refetch the project, this should see the update
	project = MustGetProject(id)

	table = project.tables["tab_update_b"]
	assert.Equal(t, table.Name, "tab_update_b")

	assert.Equal(t, len(table.Columns), 3)
	assert.Equal(t, table.Columns[0].Name, "c1_b")
	assert.Equal(t, table.Columns[0].Nullable, true)
	assert.Equal(t, table.Columns[0].Type, data.COLUMN_TYPE_TEXT)

	assert.Equal(t, table.Columns[1].Name, "c3")
	assert.Equal(t, table.Columns[1].Nullable, true)
	assert.Equal(t, table.Columns[1].Type, data.COLUMN_TYPE_REAL)

	assert.Equal(t, table.Columns[2].Name, "c5")
	assert.Equal(t, table.Columns[2].Nullable, false)
	assert.Equal(t, table.Columns[2].Type, data.COLUMN_TYPE_INT)

	assert.Equal(t, table.Access.Select.CTE, "select 1")
	assert.Equal(t, table.Access.Select.Name, "sqlkite_cte_tab_update_b")
}

func Test_Project_DeleteTable_UnknownTable(t *testing.T) {
	project := MustGetProject(tests.Factory.StandardId)
	env := project.Env()
	err := project.DeleteTable(env, "tab_nope")
	assert.Nil(t, err)
	assert.Validation(t, env.Validator).Field("", 302_033, map[string]any{"value": "tab_nope"})
}

func Test_Project_DeleteTable_Success(t *testing.T) {
	id := tests.Factory.DynamicId()
	project := MustGetProject(id)
	err := project.CreateTable(project.Env(), data.Table{
		Name: "tab_delete",
		Columns: []data.Column{
			data.BuildColumn().Name("c1").Type("text").Nullable().Column(),
		},
	})
	assert.Nil(t, err)

	project = MustGetProject(id)
	assert.Nil(t, project.DeleteTable(project.Env(), "tab_delete"))

	project = MustGetProject(id)
	table := project.Table("tab_delete")
	assert.Nil(t, table)
}

func dynamicProject() *Project {
	return MustGetProject(tests.Factory.DynamicId())
}

func Test_ApplyTableChanges(t *testing.T) {
	t1 := &data.Table{
		Name: "test1",
		Columns: []data.Column{
			data.BuildColumn().Name("c1").Column(),
			data.BuildColumn().Name("c2").Column(),
			data.BuildColumn().Name("c3").Column(),
			data.BuildColumn().Name("c4").Column(),
		},
	}

	t2 := applyTableChanges(*t1, sql.AlterTable{
		Changes: []sql.AlterTableChange{
			sql.RenameTable{To: "test2"},
			sql.DropColumn{Name: "c2"},
			sql.RenameColumn{Name: "c4", To: "c4-b"},
			sql.AddColumn{Column: data.BuildColumn().Name("c5").Type("blob").NotNullable().Column()},
		}},
		data.TableAccess{
			Select: &data.SelectTableAccess{CTE: "select 1"},
		},
	)

	assert.Equal(t, t2.Name, "test2")
	assert.Equal(t, t2.Access.Select.CTE, "select 1")

	assert.Equal(t, len(t2.Columns), 4)
	assert.Equal(t, t2.Columns[0].Name, "c1")
	assert.Equal(t, t2.Columns[1].Name, "c3")
	assert.Equal(t, t2.Columns[2].Name, "c4-b")
	assert.Equal(t, t2.Columns[3].Name, "c5")
	assert.Nil(t, t2.Columns[3].Default)
	assert.Equal(t, t2.Columns[3].Nullable, false)
	assert.Equal(t, t2.Columns[3].Type, data.COLUMN_TYPE_BLOB)

	// make sure our drop column correct modified our column array
	t3 := applyTableChanges(*t2, sql.AlterTable{
		Changes: []sql.AlterTableChange{
			sql.DropColumn{Name: "c1"},
			sql.DropColumn{Name: "c5"},
			sql.DropColumn{Name: "c3"},
		}}, data.TableAccess{})

	assert.Equal(t, len(t3.Columns), 1)
	assert.Equal(t, t3.Access.Select.CTE, "select 1")
	assert.Equal(t, t3.Columns[0].Name, "c4-b")

	// Make sure our drop column correct modified our column array.
	// Delete our select CTE
	t4 := applyTableChanges(*t3, sql.AlterTable{
		Changes: []sql.AlterTableChange{
			sql.DropColumn{Name: "c4-b"},
		}},
		data.TableAccess{
			Select: &data.SelectTableAccess{CTE: ""},
		},
	)

	assert.Nil(t, t4.Access.Select)
	assert.Equal(t, len(t4.Columns), 0)
}

func MustGetProject(id string) *Project {
	project, err := Projects.Get(id)
	if err != nil {
		panic(err)
	}
	if project == nil {
		panic("Project " + id + " does not exist")
	}
	return project
}
