package tables

import (
	"encoding/base64"
	"strings"

	"src.goblgobl.com/utils/ascii"
	"src.goblgobl.com/utils/typed"
	"src.goblgobl.com/utils/validation"
	"src.sqlkite.com/sqlkite"
	"src.sqlkite.com/sqlkite/codes"
)

var (
	// column and table names
	dataFieldPattern    = "^[a-zA-Z_][a-zA-Z0-9_]*$"
	dataFieldError      = "must begin with a letter or underscore, and only contain letters, numbers or underscores"
	tableNameValidation = validation.String[*sqlkite.Env]().
				Required().
				Length(1, 100).
				Pattern(dataFieldPattern, dataFieldError).
				Func(validateTableNamePrefix)

	columnNameValidation = validation.String[*sqlkite.Env]().
				Required().
				Length(1, 100).
				Pattern(dataFieldPattern, dataFieldError)

	columnValidation = validation.Object[*sqlkite.Env]().
				Required().
				Field("name", columnNameValidation).
				Field("type", validation.String[*sqlkite.Env]().Required().Choice("text", "int", "real", "blob").Func(columnTypeConverter)).
				Field("nullable", validation.Bool[*sqlkite.Env]().Required()).
				Field("default", validation.Any[*sqlkite.Env]().Func(validateDefault)).
				Field("unique", validation.Bool[*sqlkite.Env]().Default(false)).
				Field("autoincrement", validation.String[*sqlkite.Env]().Choice("strict", "reuse").Func(autoIncrementValidation))

	columnsValidation = validation.Array[*sqlkite.Env]().Min(1).Max(100).Required().Validator(columnValidation)

	primaryKeyValidation = validation.Array[*sqlkite.Env]().Max(5).Validator(columnNameValidation)

	mutateAccessValiation = validation.Object[*sqlkite.Env]().
				Field("when", validation.String[*sqlkite.Env]().Transform(strings.TrimSpace).Length(0, 4096)).
				Field("trigger", validation.String[*sqlkite.Env]().Transform(strings.TrimSpace).Length(1, 4096))

	accessValidation = validation.Object[*sqlkite.Env]().
				Field("select", validation.String[*sqlkite.Env]().Nullable().Transform(strings.TrimSpace).Length(0, 4096)).
				Field("insert", mutateAccessValiation).
				Field("update", mutateAccessValiation).
				Field("delete", mutateAccessValiation)

	maxMutateCountValidation = validation.Int[*sqlkite.Env]().Min(0)
)

var (
	valAutoIncrementNonInt = &validation.Invalid{
		Code:  codes.VAL_AUTOINCREMENT_NOT_INT,
		Error: "Autoincrement can only be defined on a column of type 'int'",
	}

	valAutoIncrementMultiplePK = &validation.Invalid{
		Code:  codes.VAL_AUTOINCREMENT_COMPOSITE_PK,
		Error: "Autoincrement cannot be used as part of a composite primary key",
	}

	valAutoIncrementNonPK = &validation.Invalid{
		Code:  codes.VAL_AUTOINCREMENT_NON_PK,
		Error: "Autoincrement can only be used on the primary key",
	}

	valBlobDefault = &validation.Invalid{
		Code:  codes.VAL_NON_BASE64_COLUMN_DEFAULT,
		Error: "blob default should be base64 encoded",
	}
)

func validateTableNamePrefix(name string, ctx *validation.Context[*sqlkite.Env]) any {
	if ascii.HasPrefixIgnoreCase(name, "sqlkite_") {
		ctx.InvalidField(&validation.Invalid{
			Code:  codes.VAL_RESERVED_TABLE_NAME,
			Error: "Table name cannot start with 'sqlkite_'",
			Data:  validation.ValueData(name),
		})
	}
	return name
}

func validateDefault(value any, ctx *validation.Context[*sqlkite.Env]) any {
	if value == nil {
		return nil // no default, no problem
	}

	object := ctx.Object
	tpe, ok := object["type"].(sqlkite.ColumnType)
	if !ok {
		// not even a valid type
		return nil
	}

	switch tpe {
	case sqlkite.COLUMN_TYPE_TEXT:
		n, ok := value.(string)
		if !ok {
			ctx.InvalidField(validation.TypeString)
		}
		return n
	case sqlkite.COLUMN_TYPE_INT:
		// IntIf will do some sane conversion (e.g. float -> int since
		// we're coming from JSON). This is more consistent than type-checking
		// value.
		n, ok := object.IntIf("default")
		if !ok {
			ctx.InvalidField(validation.TypeInt)
		}
		return n
	case sqlkite.COLUMN_TYPE_REAL:
		// FloatIf will do some sane conversion (e.g. float -> int since
		// we're coming from JSON). This is more consistent than type-checking
		// value.
		n, ok := object.FloatIf("default")
		if !ok {
			ctx.InvalidField(validation.TypeFloat)
		}
		return n
	case sqlkite.COLUMN_TYPE_BLOB:
		n, ok := value.(string)
		if !ok {
			ctx.InvalidField(valBlobDefault)
			return value
		}
		bytes, err := base64.StdEncoding.DecodeString(n)
		if err != nil {
			ctx.InvalidField(valBlobDefault)
		}
		return bytes
	case sqlkite.COLUMN_TYPE_INVALID:
		return nil
	}

	panic("should not reach here")
}

func columnTypeConverter(value string, ctx *validation.Context[*sqlkite.Env]) any {
	switch value {
	case "text":
		return sqlkite.COLUMN_TYPE_TEXT
	case "int":
		return sqlkite.COLUMN_TYPE_INT
	case "real":
		return sqlkite.COLUMN_TYPE_REAL
	case "blob":
		return sqlkite.COLUMN_TYPE_BLOB
	}
	// cannot be valid, Choice must have already flagged it as invalid
	return sqlkite.COLUMN_TYPE_INVALID
}

func autoIncrementValidation(value string, ctx *validation.Context[*sqlkite.Env]) any {
	object := ctx.Object

	tpe, _ := object["type"].(sqlkite.ColumnType)
	// either not an int, or not even a valid type
	if tpe != sqlkite.COLUMN_TYPE_INT {
		ctx.InvalidField(valAutoIncrementNonInt)
		return value
	}

	// Autoincrement can only be added to a column if it's the only primary key.
	pk := ctx.Input.Strings("primary_key")
	if len(pk) > 1 {
		ctx.InvalidField(valAutoIncrementMultiplePK)
		return value
	} else if len(pk) == 1 {
		if pk[0] != object.String("name") {
			ctx.InvalidField(valAutoIncrementNonPK)
			return value
		}
	}

	// it's ok if there is 0 PKs, it just means that this column (the one with the
	// "autoincrement" attribute will be the PK)

	// TODO: we should validate that there are not multiple auto-increment columns!

	switch value {
	case "strict":
		return sqlkite.AUTO_INCREMENT_TYPE_STRICT
	case "reuse":
		return sqlkite.AUTO_INCREMENT_TYPE_REUSE
	}
	// cannot be valid, Choice must have already flagged it as invalid
	return sqlkite.AUTO_INCREMENT_TYPE_NONE
}

func createTableAccessMutate(tableName string, tpe sqlkite.TableAccessMutateType, input typed.Typed) *sqlkite.TableAccessMutate {
	return sqlkite.NewTableAccessMutate(tableName, tpe, input.String("trigger"), input.String("when"))
}
