package tables

import (
	"github.com/valyala/fasthttp"
	"src.goblgobl.com/utils/http"
	"src.goblgobl.com/utils/optional"
	"src.goblgobl.com/utils/typed"
	"src.goblgobl.com/utils/validation"
	"src.sqlkite.com/sqlkite"
)

var (
	createValidation = validation.Object[*sqlkite.Env]().
		Field("name", tableNameValidation).
		Field("columns", columnsValidation).
		Field("access", accessValidation).
		Field("primary_key", primaryKeyValidation).
		Field("max_delete_count", maxMutateCountValidation).
		Field("max_update_count", maxMutateCountValidation)
)

func Create(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
	input, err := typed.Json(conn.PostBody())
	if err != nil {
		return http.InvalidJSON, nil
	}

	vc := env.VC
	if !createValidation.ValidateInput(input, vc) {
		return http.Validation(vc), nil
	}

	tableName := input.String("name")
	columns := mapColumns(input.Objects("columns"))

	var access sqlkite.TableAccess
	if input, exists := input.ObjectIf("access"); exists {
		if cte, ok := input.StringIf("select"); ok {
			access.Select = &sqlkite.TableAccessSelect{CTE: cte}
		}

		if input, ok := input.ObjectIf("insert"); ok {
			access.Insert = createTableAccessMutate(tableName, sqlkite.TABLE_ACCESS_MUTATE_INSERT, input)
		}

		if input, ok := input.ObjectIf("update"); ok {
			access.Update = createTableAccessMutate(tableName, sqlkite.TABLE_ACCESS_MUTATE_UPDATE, input)
		}

		if input, ok := input.ObjectIf("delete"); ok {
			access.Delete = createTableAccessMutate(tableName, sqlkite.TABLE_ACCESS_MUTATE_DELETE, input)
		}
	}

	err = env.Project.CreateTable(env, &sqlkite.Table{
		Name:           tableName,
		Access:         access,
		Columns:        columns,
		PrimaryKey:     input.Strings("primary_key"),
		MaxDeleteCount: input.OptionalInt("max_delete_count"),
		MaxUpdateCount: input.OptionalInt("max_update_count"),
	})

	// possible that CreateTable added validation errors
	if !vc.IsValid() {
		return http.Validation(vc), nil
	}

	return http.OK(nil), err
}

func mapColumns(input []typed.Typed) []sqlkite.Column {
	columns := make([]sqlkite.Column, len(input))
	for i, ci := range input {
		columns[i] = mapColumn(ci)
	}
	return columns
}

func mapColumn(input typed.Typed) sqlkite.Column {
	c := sqlkite.Column{
		Name:     input.String("name"),
		Nullable: input.Bool("nullable"),
		Default:  input["default"],
		Type:     input["type"].(sqlkite.ColumnType),
		Extended: new(sqlkite.ColumnExtended),
	}

	if ai, ok := input["autoincrement"].(sqlkite.AutoIncrementType); ok {
		c.Extended.AutoIncrement = optional.New(ai)
	}

	return c
}
