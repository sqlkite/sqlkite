package sql

import (
	"testing"

	"src.goblgobl.com/utils/optional"
)

func Test_Update_Simple(t *testing.T) {
	assertSQL(t, Update{
		Target: "tab1",
		Set: []UpdateSet{
			UpdateSet{Column: "c1", Value: DataField{Name: "?1"}},
		},
	}, "update tab1 set c1 = ?1")

	assertSQL(t, Update{
		Target: "tab1",
		Set: []UpdateSet{
			UpdateSet{Column: "c1", Value: DataField{Name: "?1"}},
			UpdateSet{Column: "c2", Value: DataField{Name: "?2"}},
		},
	}, "update tab1 set c1 = ?1, c2 = ?2")
}

func Test_Update_WhereAndLimit(t *testing.T) {
	assertSQL(t, Update{
		Target: "tab1",
		Set: []UpdateSet{
			UpdateSet{Column: "c1", Value: DataField{Name: "?1"}},
			UpdateSet{Column: "c2", Value: DataField{Name: "?2"}},
		},
		Limit: optional.Int(33),
		Where: Condition{
			Parts: []Part{Predicate{
				Left:  DataField{Name: "enabled"},
				Op:    []byte(" = "),
				Right: DataField{Name: "?3"},
			}},
		},
	}, "update tab1 set c1 = ?1, c2 = ?2 where (enabled = ?3) limit 33")
}

func Test_Update_From(t *testing.T) {
	assertSQL(t, Update{
		Target: "tab1",
		Set: []UpdateSet{
			UpdateSet{Column: "id", Value: DataField{Name: "id", Table: "tab2"}},
			UpdateSet{Column: "id2", Value: DataField{Name: "id2", Table: "t3"}},
		},
		Froms: []SelectFrom{
			SelectFrom{From: From{"tab2", nil}},
			SelectFrom{
				From: From{"tab3", &Alias{"t3"}},
				Join: JOIN_TYPE_INNER,
				On: &Condition{
					Parts: []Part{Predicate{
						Left:  DataField{Name: "id", Table: "tab1"},
						Op:    []byte(" = "),
						Right: DataField{Name: "id", Table: "t3"},
					}},
				},
			},
		},
		Limit: optional.Int(33),
		Where: Condition{
			Parts: []Part{Predicate{
				Left:  DataField{Name: "enabled"},
				Op:    []byte(" = "),
				Right: DataField{Name: "?3"},
			}},
		},
	}, "update tab1 set id = tab2.id, id2 = t3.id2 from tab2 inner join tab3 as t3 on (tab1.id = t3.id) where (enabled = ?3) limit 33")
}
