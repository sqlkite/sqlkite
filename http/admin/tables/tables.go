package tables

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"

	"src.goblgobl.com/sqlite"
	"src.goblgobl.com/sqlkite"
	"src.goblgobl.com/sqlkite/codes"
	"src.goblgobl.com/sqlkite/data"
	"src.goblgobl.com/utils/ascii"
	"src.goblgobl.com/utils/http"
	"src.goblgobl.com/utils/log"
	"src.goblgobl.com/utils/typed"
	"src.goblgobl.com/utils/uuid"
	"src.goblgobl.com/utils/validation"
)

var (
	// column and table names
	dataFieldPattern    = "^[a-zA-Z_][a-zA-Z0-9_]*$"
	dataFieldError      = "must begin with a letter or underscore, and only contain letters, numbers or underscores"
	tableNameValidation = validation.String().
				Required().
				Length(1, 100).
				Pattern(dataFieldPattern, dataFieldError).
				Func(validateTableNamePrefix) // this also lowercases our name

	columnNameValidation = validation.String().
				Required().
				Length(1, 100).
				Pattern(dataFieldPattern, dataFieldError).
				Transformer(ascii.Lowercase)

	columnValidation = validation.Object().
				Required().
				Field("name", columnNameValidation).
				Field("type", validation.String().Required().Choice("text", "int", "real", "blob").Convert(columnTypeConverter)).
				Field("nullable", validation.Bool().Required()).
				Field("default", validation.Any().Func(validateDefault))

	columnsValidation = validation.Array().Min(1).Max(100).Required().Validator(columnValidation)

	accessValidation = validation.Object().
				Field("select", validation.String().Length(0, 4096))

	blobDefaultError = validation.Invalid{
		Code:  codes.VAL_NON_BASE64_COLUMN_DEFAULT,
		Error: "blob default should be base64 encoded",
	}

	sqliteErrorLogData = log.NewField().
				Int("code", codes.RES_DATABASE_ERROR).
				Int("status", 500).
				Finalize()
)

func validateTableNamePrefix(field validation.Field, value string, object typed.Typed, input typed.Typed, res *validation.Result) string {
	name := ascii.Lowercase(value)
	if strings.HasPrefix(name, "sqlkite_") {
		res.AddInvalidField(field, validation.Invalid{
			Code:  codes.VAL_RESERVED_TABLE_NAME,
			Error: "Table name cannot start with 'sqlkite_'",
			Data:  validation.Value(value),
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

// In a perfect world, we'd have distinct error codes and messages for
// every possible error. And we'd be able to fully validate all the inputs
// precisely (like, you can't use the greater than operator on a text column).
// Unfortunately, that's not something we can reasonably do in all cases and
// sqlite itself gives pretty generic error codes.
// If err is an sqlite.Error, there's a reasonable chance this is a user-error.
// We still want to log the error, and we still want to return a 500, but we want
// this to be a project-level error, not a system-level error.
// (Ideally, we should have 0 system-level errors, but project level errors are
// outside of our control so we don't want to log the two the same way)
func handleError(env *sqlkite.Env, err error) (http.Response, error) {
	var sqliteErr sqlite.Error
	if !errors.As(err, &sqliteErr) {
		return nil, err
	}

	errorId := uuid.String()
	env.Error("sqlite_error").Err(err).String("eid", errorId).Log()

	data := struct {
		Code    int    `json:"code"`
		Error   string `json:"error"`
		ErrorId string `json:"error_id"`
	}{
		ErrorId: errorId,
		Code:    codes.RES_DATABASE_ERROR,
		Error:   "database error",
	}
	body, _ := json.Marshal(data)

	return http.ErrorIdResponse{
		Body:    body,
		ErrorId: errorId,
		LogData: sqliteErrorLogData,
	}, nil
}
