package sql

import (
	"testing"

	"src.goblgobl.com/utils/optional"
)

func Test_SelectFrom_Write(t *testing.T) {
	assertSQL(t, SelectFrom{
		From: From{"table1", nil},
	}, "table1")

	assertSQL(t, SelectFrom{
		From: From{"table1", &Alias{Alias: "t1"}},
	}, "table1 as t1")

	assertSQL(t, SelectFrom{
		Join: JOIN_TYPE_INNER,
		From: From{"table1", nil},
	}, "inner join table1")

	assertSQL(t, SelectFrom{
		Join: JOIN_TYPE_LEFT,
		From: From{"table1", nil},
	}, "left join table1")

	assertSQL(t, SelectFrom{
		Join: JOIN_TYPE_RIGHT,
		From: From{"table1", nil},
	}, "right join table1")

	assertSQL(t, SelectFrom{
		Join: JOIN_TYPE_FULL,
		From: From{"table1", &Alias{Alias: "tt"}},
	}, "full join table1 as tt")

	// join conditions (e.g. "on ...") are tested in http/sql/parser
}

func Test_Select_Write_NoWhere_NoOrderBy(t *testing.T) {
	assertSQL(t, Select{
		Columns: []DataField{DataField{Name: "id"}},
		Froms: []SelectFrom{
			SelectFrom{From: From{"table1", nil}},
		},
		Limit: 100,
	}, "select json_object('id', id) from table1 where true limit 100")
}

func Test_Select_Write_SingleColumn_SingleFrom(t *testing.T) {
	assertSQL(t, Select{
		Columns: []DataField{DataField{Name: "id"}},
		Froms: []SelectFrom{
			SelectFrom{From: From{"table1", nil}},
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
	assertSQL(t, Select{
		Columns: []DataField{
			DataField{Name: "id", Table: "t1"},
			DataField{Name: "full_name", Table: "t2", Alias: &Alias{"name"}},
		},
		Froms: []SelectFrom{
			SelectFrom{From: From{"table1", &Alias{"t1"}}},
			SelectFrom{
				From: From{"table2", &Alias{"t2"}},
				Join: JOIN_TYPE_INNER,
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