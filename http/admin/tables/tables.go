package tables

import (
	"encoding/base64"
	"regexp"
	"strings"

	"src.goblgobl.com/utils/ascii"
	"src.goblgobl.com/utils/optional"
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

	columnPermssionValidation = validation.Int[*sqlkite.Env]().Min(0).Max(1)

	columnValidation = validation.Object[*sqlkite.Env]().
				Required().
				Field("name", columnNameValidation).
				Field("type", validation.String[*sqlkite.Env]().Required().Choice("text", "int", "real", "blob").Func(columnTypeConverter)).
				Field("nullable", validation.Bool[*sqlkite.Env]().Required()).
				Field("default", validation.Any[*sqlkite.Env]().Func(validateDefault)).
				Field("unique", validation.Bool[*sqlkite.Env]().Default(false)).
				Field("insert_access", columnPermssionValidation).
				Field("update_access", columnPermssionValidation).
				Field("extension", validation.Object[*sqlkite.Env]().Func(validateExtension).Default(sqlkite.ColumnNoopExtension{}))

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

	intExtensionValidation = validation.Object[*sqlkite.Env]().
				Func(intExtensionConverter).
				Field("min", validation.Int[*sqlkite.Env]()).
				Field("max", validation.Int[*sqlkite.Env]()).
				Field("autoincrement", validation.String[*sqlkite.Env]().Choice("strict", "reuse").Func(autoIncrementValidation)).
				ForceField(validation.BuildField("columns.#.extension"))

	floatExtensionValidation = validation.Object[*sqlkite.Env]().
					Func(floatExtensionConverter).
					Field("min", validation.Float[*sqlkite.Env]()).
					Field("max", validation.Float[*sqlkite.Env]()).
					ForceField(validation.BuildField("columns.#.extension"))

	textExtensionValidation = validation.Object[*sqlkite.Env]().
				Func(textExtensionConverter).
				Field("min", validation.Int[*sqlkite.Env]()).
				Field("max", validation.Int[*sqlkite.Env]()).
				Field("pattern", validation.String[*sqlkite.Env]().Max(50).Func(patternValidation)).
				Field("choices", validation.Array[*sqlkite.Env]().Max(50).Validator(validation.String[*sqlkite.Env]().Max(50)).ConvertToType()).
				ForceField(validation.BuildField("columns.#.extension"))

	blobExtensionValidation = validation.Object[*sqlkite.Env]().
				Func(blobExtensionConverter).
				Field("min", validation.Int[*sqlkite.Env]()).
				Field("max", validation.Int[*sqlkite.Env]()).
				ForceField(validation.BuildField("columns.#.extension"))
)

var (
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
		Error: "Blob default should be base64 encoded",
	}

	valInvalidPattern = &validation.Invalid{
		Code:  codes.VAL_INVALID_PATTERN,
		Error: "The regular expression could not be compiled",
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

	object := ctx.CurrentObject()
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

func createTableAccessMutate(tableName string, tpe sqlkite.TableAccessMutateType, input typed.Typed) *sqlkite.TableAccessMutate {
	return sqlkite.NewTableAccessMutate(tableName, tpe, input.String("trigger"), input.String("when"))
}

func validateExtension(input map[string]any, ctx *validation.Context[*sqlkite.Env]) any {
	tpe, ok := ctx.Objects()[1]["type"].(sqlkite.ColumnType)
	if !ok {
		// not even a valid type
		return nil
	}

	var extension any
	switch tpe {
	case sqlkite.COLUMN_TYPE_TEXT:
		extension, _ = ctx.Validate(nil, input, textExtensionValidation)
	case sqlkite.COLUMN_TYPE_INT:
		extension, _ = ctx.Validate(nil, input, intExtensionValidation)
	case sqlkite.COLUMN_TYPE_REAL:
		extension, _ = ctx.Validate(nil, input, floatExtensionValidation)
	case sqlkite.COLUMN_TYPE_BLOB:
		extension, _ = ctx.Validate(nil, input, blobExtensionValidation)
	}
	return extension
}

func intExtensionConverter(value map[string]any, ctx *validation.Context[*sqlkite.Env]) any {
	return &sqlkite.ColumnIntExtension{
		Min:           optional.FromAny[int](value["min"]),
		Max:           optional.FromAny[int](value["max"]),
		AutoIncrement: typed.Or(value["autoincrement"], sqlkite.AUTO_INCREMENT_TYPE_NONE),
	}
}

func floatExtensionConverter(value map[string]any, ctx *validation.Context[*sqlkite.Env]) any {
	return &sqlkite.ColumnRealExtension{
		Min: optional.FromAny[float64](value["min"]),
		Max: optional.FromAny[float64](value["max"]),
	}
}

func textExtensionConverter(value map[string]any, ctx *validation.Context[*sqlkite.Env]) any {
	return &sqlkite.ColumnTextExtension{
		Min:     optional.FromAny[int](value["min"]),
		Max:     optional.FromAny[int](value["max"]),
		Choices: typed.Or[[]string](value["choices"], nil),
		Pattern: typed.Or(value["pattern"], ""),
	}
}

func blobExtensionConverter(value map[string]any, ctx *validation.Context[*sqlkite.Env]) any {
	return &sqlkite.ColumnBlobExtension{
		Min: optional.FromAny[int](value["min"]),
		Max: optional.FromAny[int](value["max"]),
	}
}

func autoIncrementValidation(value string, ctx *validation.Context[*sqlkite.Env]) any {
	// Autoincrement can only be added to a column if it's the only primary key.
	pk := ctx.Input.Strings("primary_key")
	if len(pk) > 1 {
		ctx.InvalidField(valAutoIncrementMultiplePK)
		return sqlkite.AUTO_INCREMENT_TYPE_NONE
	} else if len(pk) == 1 {
		if pk[0] != ctx.Objects()[1].String("name") {
			ctx.InvalidField(valAutoIncrementNonPK)
			return sqlkite.AUTO_INCREMENT_TYPE_NONE
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

func patternValidation(value string, ctx *validation.Context[*sqlkite.Env]) any {
	if _, err := regexp.Compile(value); err != nil {
		ctx.InvalidField(valInvalidPattern)
	}
	return value
}
