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
	updateValidation = validation.Object().
				Field("changes", validation.Array().Validator(validation.Object().Func(changeValidation)).Transformer(changeMap)).
				Field("access", accessValidation).
				Field("max_delete_count", maxMutateCountValidation).
				Field("max_update_count", maxMutateCountValidation)

	updateChangeToField                = validation.BuildField("changes.#.to")
	updateChangeTypeField              = validation.BuildField("changes.#.type")
	updateChangeColumnNameField        = validation.BuildField("changes.#.name")
	updateChangeColumnField            = validation.BuildField("changes.#.column")
	updateTableAlterAddColumnValidator = columnValidation.AddField(updateChangeColumnField)
)

func Update(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
	input, err := typed.Json(conn.PostBody())
	if err != nil {
		return http.InvalidJSON, nil
	}

	validator := env.Validator
	if !updateValidation.Validate(input, validator) {
		return http.Validation(validator), nil
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
	if !validator.IsValid() {
		return http.Validation(validator), nil
	}

	return http.Ok(nil), nil
}

func changeValidation(field validation.Field, value typed.Typed, input typed.Typed, res *validation.Result) any {
	switch ascii.Lowercase(value.String("type")) {
	case "rename":
		return renameValidation(field, value, res)
	case "rename column":
		return renameColumnValidaiton(field, value, res)
	case "add column":
		return addColumnValidation(field, value, res)
	case "drop column":
		return dropColumnValidation(field, value, res)
	case "":
		res.AddInvalidField(updateChangeTypeField, validation.Required())
	default:
		res.AddInvalidField(updateChangeTypeField, validation.InvalidStringChoice([]string{"rename", "rename column", "add column", "drop column"}))
	}
	return nil
}

func renameValidation(field validation.Field, change typed.Typed, res *validation.Result) any {
	tableNameValidation.ValidateObjectField(updateChangeToField, change, change, res)
	return sqlkite.TableAlterRename{
		To: change.String("to"),
	}
}

func renameColumnValidaiton(field validation.Field, change typed.Typed, res *validation.Result) any {
	columnNameValidation.ValidateObjectField(updateChangeToField, change, change, res)
	columnNameValidation.ValidateObjectField(updateChangeColumnNameField, change, change, res)
	return sqlkite.TableAlterRenameColumn{
		Name: change.String("name"),
		To:   change.String("to"),
	}
}

func addColumnValidation(field validation.Field, change typed.Typed, res *validation.Result) any {
	updateTableAlterAddColumnValidator.ValidateObjectField(updateChangeColumnField, change, change, res)
	if !res.IsValid() {
		return nil
	}
	return sqlkite.TableAlterAddColumn{
		Column: mapColumn(change.Object("column")),
	}
}

func dropColumnValidation(field validation.Field, change typed.Typed, res *validation.Result) any {
	columnNameValidation.ValidateObjectField(updateChangeColumnNameField, change, change, res)
	return sqlkite.TableAlterDropColumn{
		Name: change.String("name"),
	}
}

// Our validation has turned each change map[string]any into
// a specific sql.{Type} (e.g. sql.TableAlterRename), which are all sql.Parts.
// We need to change our array from an []any to an []sql.Part
func changeMap(changes []any) any {
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
