package tables

import (
	"github.com/valyala/fasthttp"
	"src.goblgobl.com/utils/ascii"
	"src.goblgobl.com/utils/http"
	"src.goblgobl.com/utils/kdr"
	"src.goblgobl.com/utils/typed"
	"src.goblgobl.com/utils/validation"
	"src.sqlkite.com/sqlkite"
	"src.sqlkite.com/sqlkite/sql"
)

var (
	changeValidator  = validation.Object[*sqlkite.Env]().Func(changeValidation)
	updateValidation = validation.Object[*sqlkite.Env]().
				Field("changes", validation.Array[*sqlkite.Env]().Validator(changeValidator).Func(changeMap)).
				Field("access", accessValidation).
				Field("max_delete_count", maxMutateCountValidation).
				Field("max_update_count", maxMutateCountValidation)

	updateChangeToField                = validation.BuildField("changes.#.to")
	updateChangeTypeField              = validation.BuildField("changes.#.type")
	updateChangeColumnNameField        = validation.BuildField("changes.#.name")
	updateChangeColumnField            = validation.BuildField("changes.#.column")
	updateTableAlterAddColumnValidator = columnValidation.ForceField(updateChangeColumnField)

	valChangeTypeChoice = validation.InvalidStringChoice([]string{"rename", "rename column", "add column", "drop column"})
)

func Update(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
	input, err := typed.Json(conn.PostBody())
	if err != nil {
		return http.InvalidJSON, nil
	}

	vc := env.VC
	if !updateValidation.ValidateInput(input, vc) {
		return http.Validation(vc), nil
	}

	tableName := conn.UserValue("name").(string)
	alterTable := sqlkite.TableAlter{Name: tableName}

	// Our validators have already converted input["change"] into things like
	// sqlkite.TableAlterDropColumn.
	// It's possible we have no changes to the table, maybe we only have access
	// control changes (which we treat separately because they don't have a pure
	// DDL analog)
	if changes := input["changes"]; changes != nil {
		alterTable.Changes = changes.([]sql.Part)
	}

	// tri-state:
	// keep what we have
	// delete what we have
	// replace what we have
	access := input.Object("access")
	if sa, ok := access["select"]; !ok {
		alterTable.SelectAccess = kdr.Keep[*sqlkite.TableAccessSelect]()
	} else if sa == nil {
		alterTable.SelectAccess = kdr.Delete[*sqlkite.TableAccessSelect]()
	} else {
		alterTable.SelectAccess = kdr.Replace(&sqlkite.TableAccessSelect{CTE: sa.(string)})
	}

	if ia, ok := access["insert"]; !ok {
		alterTable.InsertAccess = kdr.Keep[*sqlkite.TableAccessMutate]()
	} else if ia == nil {
		alterTable.InsertAccess = kdr.Delete[*sqlkite.TableAccessMutate]()
	} else {
		alterTable.InsertAccess = kdr.Replace(createTableAccessMutate(tableName, sqlkite.TABLE_ACCESS_MUTATE_INSERT, typed.Typed(ia.(map[string]any))))
	}

	if ua, ok := access["update"]; !ok {
		alterTable.UpdateAccess = kdr.Keep[*sqlkite.TableAccessMutate]()
	} else if ua == nil {
		alterTable.UpdateAccess = kdr.Delete[*sqlkite.TableAccessMutate]()
	} else {
		alterTable.UpdateAccess = kdr.Replace(createTableAccessMutate(tableName, sqlkite.TABLE_ACCESS_MUTATE_UPDATE, typed.Typed(ua.(map[string]any))))
	}

	if da, ok := access["delete"]; !ok {
		alterTable.DeleteAccess = kdr.Keep[*sqlkite.TableAccessMutate]()
	} else if da == nil {
		alterTable.DeleteAccess = kdr.Delete[*sqlkite.TableAccessMutate]()
	} else {
		alterTable.DeleteAccess = kdr.Replace(createTableAccessMutate(tableName, sqlkite.TABLE_ACCESS_MUTATE_DELETE, typed.Typed(da.(map[string]any))))
	}

	// The last argument, our sql.Table, only represents part of the data
	// The existing table will be loaded, and its columns will be used as a base
	// to apply the alterTable changes to it. That's why the &sql.Table that we're
	// passing doesn't need the columns.
	err = env.Project.UpdateTable(env, &sqlkite.Table{
		Name:           tableName,
		MaxDeleteCount: input.OptionalInt("max_delete_count"),
		MaxUpdateCount: input.OptionalInt("max_update_count"),
	}, alterTable)

	if err != nil {
		return nil, err
	}

	// possible that UpdateTable added validation errors
	if !vc.IsValid() {
		return http.Validation(vc), nil
	}

	return http.OK(nil), nil
}

func changeValidation(m map[string]any, ctx *validation.Context[*sqlkite.Env]) any {
	change := typed.Typed(m)
	switch ascii.Lowercase(change.String("type")) {
	case "rename":
		return renameValidation(change, ctx)
	case "rename column":
		return renameColumnValidaiton(change, ctx)
	case "add column":
		return addColumnValidation(change, ctx)
	case "drop column":
		return dropColumnValidation(change, ctx)
	case "":
		ctx.InvalidWithField(validation.Required, updateChangeTypeField)
	default:
		ctx.InvalidWithField(valChangeTypeChoice, updateChangeTypeField)
	}
	return nil
}

func renameValidation(change typed.Typed, ctx *validation.Context[*sqlkite.Env]) any {
	to, ok := ctx.Validate(updateChangeToField, change[updateChangeToField.Name], tableNameValidation)
	if !ok {
		return sqlkite.TableAlterRename{}
	}
	return sqlkite.TableAlterRename{To: to.(string)}

}

func renameColumnValidaiton(change typed.Typed, ctx *validation.Context[*sqlkite.Env]) any {
	to, ok1 := ctx.Validate(updateChangeToField, change[updateChangeToField.Name], columnNameValidation)
	name, ok2 := ctx.Validate(updateChangeColumnNameField, change[updateChangeColumnNameField.Name], columnNameValidation)
	if !ok1 || !ok2 {
		return sqlkite.TableAlterRenameColumn{}
	}

	return sqlkite.TableAlterRenameColumn{Name: name.(string), To: to.(string)}
}

func addColumnValidation(change typed.Typed, ctx *validation.Context[*sqlkite.Env]) any {
	column, ok := ctx.Validate(updateChangeColumnField, change["column"], updateTableAlterAddColumnValidator)
	if !ok {
		return sqlkite.TableAlterAddColumn{}
	}

	return sqlkite.TableAlterAddColumn{
		Column: mapColumn(typed.Typed(column.(map[string]any))),
	}
}

func dropColumnValidation(change typed.Typed, ctx *validation.Context[*sqlkite.Env]) any {
	name, ok := ctx.Validate(updateChangeColumnNameField, change[updateChangeColumnNameField.Name], columnNameValidation)

	if !ok {
		return sqlkite.TableAlterDropColumn{}
	}

	return sqlkite.TableAlterDropColumn{Name: name.(string)}
}

// Our validation has turned each change map[string]any into
// a specific sql.{Type} (e.g. sql.TableAlterRename), which are all sql.Parts.
// We need to change our array from an []any to an []sql.Part
func changeMap(changes []any, ctx *validation.Context[*sqlkite.Env]) any {
	if changes == nil {
		return nil
	}
	parts := make([]sql.Part, len(changes))
	for i, change := range changes {
		if change == nil {
			// Some (or all) of the changes were invalid, no need to do this
			return changes
		}
		parts[i] = change.(sql.Part)
	}
	return parts
}
