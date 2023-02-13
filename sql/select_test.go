package sql

import (
	"testing"

	"src.goblgobl.com/tests/assert"
	"src.goblgobl.com/utils/optional"
	"src.sqlkite.com/sqlkite/tests"
)

func Test_JoinableFrom_TableName(t *testing.T) {
	sf := JoinableFrom{Table: TableName{"table1", nil}}
	assert.Equal(t, sf.TableName(), "table1")
}

func Test_JoinableFrom_Write(t *testing.T) {
	tests.AssertSQL(t, JoinableFrom{
		Table: TableName{"table1", nil},
	}, "table1")

	tests.AssertSQL(t, JoinableFrom{
		Table: TableName{"table1", &Alias{Alias: "t1"}},
	}, "table1 as t1")

	tests.AssertSQL(t, JoinableFrom{
		Join:  JOIN_TYPE_INNER,
		Table: TableName{"table1", nil},
	}, "inner join table1")

	tests.AssertSQL(t, JoinableFrom{
		Join:  JOIN_TYPE_LEFT,
		Table: TableName{"table1", nil},
	}, "left join table1")

	tests.AssertSQL(t, JoinableFrom{
		Join:  JOIN_TYPE_RIGHT,
		Table: TableName{"table1", nil},
	}, "right join table1")

	tests.AssertSQL(t, JoinableFrom{
		Join:  JOIN_TYPE_FULL,
		Table: TableName{"table1", &Alias{Alias: "tt"}},
	}, "full join table1 as tt")

	// join conditions (e.g. "on ...") are tested in http/sql/parser
}

func Test_Select_Write_NoWhere_NoOrderBy(t *testing.T) {
	tests.AssertSQL(t, Select{
		Columns: []DataField{DataField{Name: "id"}},
		Froms: []JoinableFrom{
			JoinableFrom{Table: TableName{"table1", nil}},
		},
		Limit: 100,
	}, "select json_object('id', id) from table1 limit 100")
}

func Test_Select_Write_SingleColumn_SingleFrom(t *testing.T) {
	tests.AssertSQL(t, Select{
		Columns: []DataField{DataField{Name: "id"}},
		Froms: []JoinableFrom{
			JoinableFrom{Table: TableName{"table1", nil}},
		},
		Where: Condition{
			Parts: []Part{Predicate{
				Left:  DataField{Name: "enabled"},
				Op:    []byte(" = "),
				Right: DataField{Name: "?1"},
			}},
		},
		Limit:   2,
		OrderBy: []OrderBy{OrderBy{Field: DataField{Name: "id"}}},
	}, "select json_object('id', id) from table1 where (enabled = ?1) order by id limit 2")
}

func Test_Select_Write_MultipleColumn_MultipleFrom(t *testing.T) {
	tests.AssertSQL(t, Select{
		Columns: []DataField{
			DataField{Name: "id", Table: "t1"},
			DataField{Name: "full_name", Table: "t2", Alias: &Alias{"name"}},
		},
		Froms: []JoinableFrom{
			JoinableFrom{Table: TableName{"table1", &Alias{"t1"}}},
			JoinableFrom{
				Table: TableName{"table2", &Alias{"t2"}},
				Join:  JOIN_TYPE_INNER,
				On: &Condition{
					Parts: []Part{Predicate{
						Left:  DataField{Name: "id", Table: "t1"},
						Op:    []byte(" = "),
						Right: DataField{Name: "parent_id", Table: "t2"},
					}},
				},
			},
		},
		Where: Condition{
			Parts: []Part{Predicate{
				Left:  DataField{Name: "enabled"},
				Op:    []byte(" = "),
				Right: DataField{Name: "?1"},
			}},
		},
		Limit:  3,
		Offset: optional.Int(4),
		OrderBy: []OrderBy{
			OrderBy{Field: DataField{Name: "id"}},
			OrderBy{Field: DataField{Name: "parent_id", Table: "t2"}, Desc: true},
		},
	}, "select json_object('id', t1.id, 'name', t2.full_name) from table1 as t1 inner join table2 as t2 on (t1.id = t2.parent_id) where (enabled = ?1) order by id,t2.parent_id desc limit 3 offset 4")
}

func Test_Select_Single_CTE(t *testing.T) {
	sel := Select{
		Columns: []DataField{DataField{Name: "id"}},
		Froms: []JoinableFrom{
			JoinableFrom{Table: TableName{"table1", &Alias{"t1"}}},
		},
	}

	sel.CTE(0, "table1_cte", "select * from table1 where public")
	tests.AssertSQL(t, sel, "with table1_cte as (select * from table1 where public) select json_object('id', id) from table1_cte as t1 limit 0")
}

func Test_Select_Multiple_CTE(t *testing.T) {
	sel := Select{
		Columns: []DataField{DataField{Name: "id"}},
		Froms: []JoinableFrom{
			JoinableFrom{Table: TableName{"table1", &Alias{"t1"}}},
			JoinableFrom{Join: JOIN_TYPE_LEFT, Table: TableName{"table2", nil}},
			JoinableFrom{Join: JOIN_TYPE_LEFT, Table: TableName{"table3", nil}},
		},
	}

	sel.CTE(0, "table1_cte", "select * from table1 where public")
	sel.CTE(2, "table3_cte", "select * from table3")
	tests.AssertSQL(t, sel, "with table1_cte as (select * from table1 where public), table3_cte as (select * from table3) select json_object('id', id) from table1_cte as t1 left join table2 left join table3_cte limit 0")
}
