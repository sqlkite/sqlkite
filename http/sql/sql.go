package sql

import (
	"encoding/json"
	"errors"
	"fmt"

	"src.goblgobl.com/sqlite"
	"src.goblgobl.com/sqlkite"
	"src.goblgobl.com/sqlkite/codes"
	"src.goblgobl.com/utils/http"
	"src.goblgobl.com/utils/log"
	"src.goblgobl.com/utils/uuid"
	"src.goblgobl.com/utils/validation"
)

const (
	FROM_INPUT_NAME       = "from"
	WHERE_INPUT_NAME      = "where"
	ORDER_INPUT_NAME      = "order"
	LIMIT_INPUT_NAME      = "limit"
	OFFSET_INPUT_NAME     = "offset"
	SELECT_INPUT_NAME     = "select"
	PARAMETERS_INPUT_NAME = "parameters"
)

// The validation of this package is different and more messy. We don't
// just validate the input as a whole, but rather validate on a field-by-
// field (and sometimes part-by-part) basis as part of parsing. Or, to
// say it differently, we validate as part of parsing.
var (
	valRequired  = validation.Required()
	valIntType   = validation.InvalidIntType()
	valArrayType = validation.InvalidArrayType()

	fromField       = validation.NewIndexedField(FROM_INPUT_NAME)
	whereField      = validation.NewIndexedField(WHERE_INPUT_NAME)
	orderField      = validation.NewIndexedField(ORDER_INPUT_NAME)
	limitField      = validation.NewIndexedField(LIMIT_INPUT_NAME)
	offsetField     = validation.NewIndexedField(OFFSET_INPUT_NAME)
	selectField     = validation.NewIndexedField(SELECT_INPUT_NAME)
	parametersField = validation.NewIndexedField(PARAMETERS_INPUT_NAME)

	sqliteErrorLogData = log.NewField().
				Int("code", codes.RES_DATABASE_ERROR).
				Int("status", 500).
				Finalize()
)

func extractParameters(input any, validator *validation.Result, p *sqlkite.Project) []any {
	if input == nil {
		return nil
	}

	values, ok := input.([]any)
	if !ok {
		validator.AddInvalidField(parametersField, valArrayType)
	}

	max := int(p.MaxSQLParameterCount)
	if len(values) > max {
		validator.AddInvalidField(parametersField, validation.Invalid{
			Code:  codes.VAL_SQL_TOO_MANY_PARAMETERS,
			Error: fmt.Sprintf("must have no more than %d values", max),
			Data:  validation.Max(max),
		})
	}

	return values
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
