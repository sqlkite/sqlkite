package tables

import (
	"github.com/valyala/fasthttp"
	"src.goblgobl.com/sqlkite"
	"src.goblgobl.com/sqlkite/sql"
	"src.goblgobl.com/utils/ascii"
	"src.goblgobl.com/utils/http"
	"src.goblgobl.com/utils/typed"
	"src.goblgobl.com/utils/validation"
)

var (
	updateValidation = validation.Object().
				Field("changes", validation.Array().Validator(validation.Object().Func(changeValidation)).Transformer(changeMap)).
				Field("access", accessValidation)

	updateChangeToField         = validation.BuildField("changes.#.to")
	updateChangeTypeField       = validation.BuildField("changes.#.type")
	updateChangeColumnNameField = validation.BuildField("changes.#.name")
	updateChangeColumnField     = validation.BuildField("changes.#.column")
	updateAddColumnValidator    = columnValidation.AddField(updateChangeColumnField)
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

	// input["change"] has already been converted to things like sql.RenameTable
	// and sql.DropColumn in the validation
	// It's possible we have no changes to the table, maybe we only have access
	// control changes (which we treat separately because they don't have a pure
	// DDL analog)
	alterTable := sql.AlterTable{
		Name: ascii.Lowercase(conn.UserValue("name").(string)),
	}
	if changes := input["changes"]; changes != nil {
		alterTable.Changes = changes.([]sql.Part)
	}

	access := mapAccess(input.Object("access"))

	err = env.Project.UpdateTable(env, alterTable, access)

	// possible that UpdateTable added validation errors
	if !validator.IsValid() {
		return http.Validation(validator), nil
	}

	return http.Ok(nil), err
}

func changeValidation(field validation.Field, value typed.Typed, input typed.Typed, res *validation.Result) any {
	switch ascii.Lowercase(value.String("type")) {
	case "rename":
		return renameValidation(field, value, res)
	case "rename column":
		return renameColumnValidaiton(field, value, res)
	case "add column":
		return addColumnValidaiton(field, value, res)
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
	return sql.RenameTable{
		To: change.String("to"),
	}
}

func renameColumnValidaiton(field validation.Field, change typed.Typed, res *validation.Result) any {
	columnNameValidation.ValidateObjectField(updateChangeToField, change, change, res)
	columnNameValidation.ValidateObjectField(updateChangeColumnNameField, change, change, res)
	return sql.RenameColumn{
		Name: change.String("name"),
		To:   change.String("to"),
	}
}

func addColumnValidaiton(field validation.Field, change typed.Typed, res *validation.Result) any {
	updateAddColumnValidator.ValidateObjectField(updateChangeColumnField, change, change, res)
	return nil
}

func dropColumnValidation(field validation.Field, change typed.Typed, res *validation.Result) any {
	columnNameValidation.ValidateObjectField(updateChangeColumnNameField, change, change, res)
	return sql.DropColumn{
		Name: change.String("name"),
	}
}

// Our validation has turned each change map[string]any into
// a specific sql.{Type} (e.g. sql.RenameTable), which are all sql.Parts.
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
