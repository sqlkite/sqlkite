package tables

import (
	"encoding/base64"

	"src.goblgobl.com/utils/ascii"
	"src.goblgobl.com/utils/typed"
	"src.goblgobl.com/utils/validation"
	"src.sqlkite.com/sqlkite"
	"src.sqlkite.com/sqlkite/codes"
	"src.sqlkite.com/sqlkite/data"
)

var (
	// column and table names
	dataFieldPattern    = "^[a-zA-Z_][a-zA-Z0-9_]*$"
	dataFieldError      = "must begin with a letter or underscore, and only contain letters, numbers or underscores"
	tableNameValidation = validation.String().
				Required().
				Length(1, 100).
				Pattern(dataFieldPattern, dataFieldError).
				Func(validateTableNamePrefix)

	columnNameValidation = validation.String().
				Required().
				Length(1, 100).
				Pattern(dataFieldPattern, dataFieldError)

	columnValidation = validation.Object().
				Required().
				Field("name", columnNameValidation).
				Field("type", validation.String().Required().Choice("text", "int", "real", "blob").Convert(columnTypeConverter)).
				Field("nullable", validation.Bool().Required()).
				Field("default", validation.Any().Func(validateDefault))

	columnsValidation = validation.Array().Min(1).Max(100).Required().Validator(columnValidation)

	mutateAccessValiation = validation.Object().
				Field("when", validation.String().TrimSpace().Length(0, 4096)).
				Field("trigger", validation.String().TrimSpace().Length(0, 4096))

	accessValidation = validation.Object().
				Field("select", validation.String().TrimSpace().Length(0, 4096)).
				Field("insert", mutateAccessValiation).
				Field("update", mutateAccessValiation).
				Field("delete", mutateAccessValiation)

	maxMutateCountValidation = validation.Int().Min(0)
	maxSelectCountValidation = validation.Int().Min(0).Max(65536)
	maxSelectCountField      = validation.NewField("max_select_count")

	blobDefaultError = validation.Invalid{
		Code:  codes.VAL_NON_BASE64_COLUMN_DEFAULT,
		Error: "blob default should be base64 encoded",
	}
)

func validateTableNamePrefix(field validation.Field, name string, object typed.Typed, input typed.Typed, res *validation.Result) string {
	if ascii.HasPrefixIgnoreCase(name, "sqlkite_") {
		res.AddInvalidField(field, validation.Invalid{
			Code:  codes.VAL_RESERVED_TABLE_NAME,
			Error: "Table name cannot start with 'sqlkite_'",
			Data:  validation.Value(name),
		})
	}
	return name
}

func validateDefault(field validation.Field, value any, object typed.Typed, input typed.Typed, res *validation.Result) any {
	if value == nil {
		return nil // no default, no problem
	}

	switch object["type"].(data.ColumnType) {
	case data.COLUMN_TYPE_TEXT:
		n, ok := value.(string)
		if !ok {
			res.AddInvalidField(field, validation.InvalidStringType())
		}
		return n
	case data.COLUMN_TYPE_INT:
		// IntIf will do some sane conversion (e.g. float -> int since
		// we're coming from JSON). This is more consistent than type-checking
		// value.
		n, ok := object.IntIf("default")
		if !ok {
			res.AddInvalidField(field, validation.InvalidIntType())
		}
		return n
	case data.COLUMN_TYPE_REAL:
		// FloatIf will do some sane conversion (e.g. float -> int since
		// we're coming from JSON). This is more consistent than type-checking
		// value.
		n, ok := object.FloatIf("default")
		if !ok {
			res.AddInvalidField(field, validation.InvalidFloatType())
		}
		return n
	case data.COLUMN_TYPE_BLOB:
		n, ok := value.(string)
		if !ok {
			res.AddInvalidField(field, blobDefaultError)
			return value
		}
		bytes, err := base64.StdEncoding.DecodeString(n)
		if err != nil {
			res.AddInvalidField(field, blobDefaultError)
		}
		return bytes
	case data.COLUMN_TYPE_INVALID:
		return nil
	}

	panic("should not reach here")
}

func columnTypeConverter(field validation.Field, value string, object typed.Typed, input typed.Typed, res *validation.Result) any {
	switch value {
	case "text":
		return data.COLUMN_TYPE_TEXT
	case "int":
		return data.COLUMN_TYPE_INT
	case "real":
		return data.COLUMN_TYPE_REAL
	case "blob":
		return data.COLUMN_TYPE_BLOB
	}
	// cannot be valid, Choice must have already flagged it as invalid
	return data.COLUMN_TYPE_INVALID
}

func maxSelectCount(env *sqlkite.Env, arg int) uint16 {
	projectMax := env.Project.MaxSelectCount
	if arg == -1 {
		return projectMax
	}

	if uint16(arg) <= projectMax {
		return uint16(arg)
	}

	env.Validator.AddInvalidField(maxSelectCountField, validation.Invalid{
		Code:  codes.VAL_TABLE_LIMIT_EXCEEDS_PROJECT_LIMIT,
		Error: "The table select limit cannot exceed the project-wide select limit",
		Data:  validation.Max(projectMax),
	})

	return projectMax
}
