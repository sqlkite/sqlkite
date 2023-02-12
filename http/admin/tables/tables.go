package tables

import (
	"encoding/base64"

	"src.goblgobl.com/utils/ascii"
	"src.goblgobl.com/utils/typed"
	"src.goblgobl.com/utils/validation"
	"src.sqlkite.com/sqlkite/codes"
	"src.sqlkite.com/sqlkite/sql"
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
				Field("default", validation.Any().Func(validateDefault)).
				Field("autoincrement", validation.String().Choice("strict", "reuse").Func(autoIncrementValidation).Convert(autoIncrementConverter))

	columnsValidation = validation.Array().Min(1).Max(100).Required().Validator(columnValidation)

	primaryKeyValidation = validation.Array().Max(5).Validator(columnNameValidation)

	mutateAccessValiation = validation.Object().
				Field("when", validation.String().TrimSpace().Length(0, 4096)).
				Field("trigger", validation.String().TrimSpace().Length(0, 4096))

	accessValidation = validation.Object().
				Field("select", validation.String().TrimSpace().Length(0, 4096)).
				Field("insert", mutateAccessValiation).
				Field("update", mutateAccessValiation).
				Field("delete", mutateAccessValiation)

	maxMutateCountValidation = validation.Int().Min(0)

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

	switch object["type"].(sql.ColumnType) {
	case sql.COLUMN_TYPE_TEXT:
		n, ok := value.(string)
		if !ok {
			res.AddInvalidField(field, validation.InvalidStringType())
		}
		return n
	case sql.COLUMN_TYPE_INT:
		// IntIf will do some sane conversion (e.g. float -> int since
		// we're coming from JSON). This is more consistent than type-checking
		// value.
		n, ok := object.IntIf("default")
		if !ok {
			res.AddInvalidField(field, validation.InvalidIntType())
		}
		return n
	case sql.COLUMN_TYPE_REAL:
		// FloatIf will do some sane conversion (e.g. float -> int since
		// we're coming from JSON). This is more consistent than type-checking
		// value.
		n, ok := object.FloatIf("default")
		if !ok {
			res.AddInvalidField(field, validation.InvalidFloatType())
		}
		return n
	case sql.COLUMN_TYPE_BLOB:
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
	case sql.COLUMN_TYPE_INVALID:
		return nil
	}

	panic("should not reach here")
}

func columnTypeConverter(field validation.Field, value string, object typed.Typed, input typed.Typed, res *validation.Result) any {
	switch value {
	case "text":
		return sql.COLUMN_TYPE_TEXT
	case "int":
		return sql.COLUMN_TYPE_INT
	case "real":
		return sql.COLUMN_TYPE_REAL
	case "blob":
		return sql.COLUMN_TYPE_BLOB
	}
	// cannot be valid, Choice must have already flagged it as invalid
	return sql.COLUMN_TYPE_INVALID
}

func autoIncrementValidation(field validation.Field, value string, object typed.Typed, input typed.Typed, res *validation.Result) string {
	if object["type"].(sql.ColumnType) != sql.COLUMN_TYPE_INT {
		res.AddInvalidField(field, validation.Invalid{
			Code:  codes.VAL_AUTOINCREMENT_NOT_INT,
			Error: "Autoincrement can only be defined on a column of type 'int'",
		})
		return value
	}

	// Autoincrement can only be added to a column if it's the only primary key.
	pk := input.Strings("primary_key")
	if len(pk) > 1 {
		res.AddInvalidField(field, validation.Invalid{
			Code:  codes.VAL_AUTOINCREMENT_COMPOSITE_PK,
			Error: "Autoincrement cannot be used as part of a composite primary key",
		})
	} else if len(pk) == 1 {
		if pk[0] != object.String("name") {
			res.AddInvalidField(field, validation.Invalid{
				Code:  codes.VAL_AUTOINCREMENT_NON_PK,
				Error: "Autoincrement can only be used on the primary key",
			})
		}
	}

	// it's ok if there is 0 PKs, it just means that this column (the one with the
	// "autoincrement" attribute will be the PK)

	// TODO: we should validate that there are not multiple auto-increment columns!
	return value
}

func autoIncrementConverter(field validation.Field, value string, object typed.Typed, input typed.Typed, res *validation.Result) any {
	switch value {
	case "strict":
		return sql.AUTO_INCREMENT_TYPE_STRICT
	case "reuse":
		return sql.AUTO_INCREMENT_TYPE_REUSE
	}
	// cannot be valid, Choice must have already flagged it as invalid
	return sql.AUTO_INCREMENT_TYPE_NONE
}
