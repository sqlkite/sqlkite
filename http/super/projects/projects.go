package projects

import (
	"src.goblgobl.com/utils/http"
	"src.goblgobl.com/utils/validation"
	"src.sqlkite.com/sqlkite/codes"
	"src.sqlkite.com/sqlkite/sql"
)

var (
	idValidation                   = validation.UUID().Required()
	maxConcurrencyValidation       = validation.Int().Default(5).Range(1, 100)
	maxSQLLengthValidation         = validation.Int().Default(4096).Range(512, 16384)
	maxSQLParameterCountValidation = validation.Int().Default(100).Range(0, sql.MAX_PARAMETERS)
	maxDatabaseSizeValidation      = validation.Int().Default(104857600).Range(1048576, 10485760000) // 1MB - 10GB, defaults to 100MB
	maxSelectCountValidation       = validation.Int().Default(100).Range(1, 10000)
	maxResultLengthValidation      = validation.Int().Default(524288).Range(1024, 5242880) // 5MB max size

	resNotFound = http.StaticError(404, codes.RES_PROJECT_NOT_FOUND, "project not found")
)
