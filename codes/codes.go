package codes

const (
	VAL_INVALID_COLUMN_NAME          = 301_001
	VAL_INVALID_ALIAS_NAME           = 301_002
	VAL_INVALID_TABLE_NAME           = 301_003
	VAL_INVALID_PREDICATE            = 301_004
	VAL_INVALID_PREDICATE_OP         = 301_005
	VAL_INVALID_PREDICATE_LEFT       = 301_006
	VAL_INVALID_PREDICATE_RIGHT      = 301_007
	VAL_INVALID_PLACEHOLDER          = 301_008
	VAL_INVALID_CONDITION_COUNT      = 301_009
	VAL_INVALID_CONDITION_LOGICAL    = 301_010
	VAL_INVALID_CONDITION_DEPTH      = 301_011
	VAL_INVALID_JOINABLE_FROM        = 301_012
	VAL_INVALID_JOINABLE_FROM_COUNT  = 301_013
	VAL_INVALID_JOINABLE_FROM_TABLE  = 301_014
	VAL_INVALID_JOINABLE_FROM_JOIN   = 301_015
	VAL_INVALID_JOINABLE_FROM_ON     = 301_016
	VAL_NON_BASE64_COLUMN_DEFAULT    = 301_017
	VAL_INVALID_COLUMN_TYPE          = 301_018
	VAL_INVALID_PREDICATE_OP_TYPE    = 301_019
	VAL_INVALID_PREDICATE_LEFT_TYPE  = 301_020
	VAL_INVALID_PREDICATE_RIGHT_TYPE = 301_021
	VAL_SQL_TOO_LONG                 = 301_022
	VAL_RESULT_TOO_LONG              = 301_023
	VAL_SQL_TOO_MANY_PARAMETERS      = 301_024
	VAL_SQL_LIMIT_TOO_HIGH           = 301_025
	VAL_TOO_MANY_FROMS               = 301_026
	VAL_INVALID_ORDER_BY_TYPE        = 301_027
	VAL_INVALID_ORDER_BY             = 301_028
	VAL_SQL_TOO_MANY_SELECT          = 301_029
	VAL_SQL_TOO_MANY_ORDER_BY        = 301_030
	VAL_SQL_TOO_MANY_CONDITIONS      = 301_031
	VAL_TOO_MANY_TABLES              = 301_032
	VAL_UNKNOWN_TABLE                = 302_033
	VAL_RESERVED_TABLE_NAME          = 302_034
	VAL_AUTOINCREMENT_NOT_INT        = 302_035
	VAL_AUTOINCREMENT_COMPOSITE_PK   = 302_036
	VAL_AUTOINCREMENT_NON_PK         = 302_037
	VAL_COMMON_PASSWORD              = 302_038
	VAL_AUTH_EMAIL_IN_USE            = 302_039

	RES_UNKNOWN_ROUTE               = 302_001
	RES_MISSING_PROJECT_HEADER      = 302_002
	RES_MISSING_SUBDOMAIN           = 302_003
	RES_PROJECT_NOT_FOUND           = 302_004
	RES_DATABASE_ERROR              = 302_005
	RES_INVALID_CREDENTIALS         = 302_006
	RES_ACCESS_DENIED               = 302_007
	RES_SESSION_INVALID_CREDENTIALS = 302_008

	ERR_READ_CONFIG                     = 303_001
	ERR_PARSE_CONFIG                    = 303_002
	ERR_INVALID_STORAGE_TYPE            = 303_003
	ERR_CONFIG_ROOT_PATH_REQUIRED       = 303_004
	ERR_CONFIG_ROOT_PATH_INVALID        = 303_005
	ERR_CONFIG_ROOT_PATH_FILE           = 303_006
	ERR_CONFIG_HTTP_PROJECT_TYPE        = 303_007
	ERR_CONFIG_HTTP_AUTHENTICATION_TYPE = 303_008
	ERR_CONFIG_HTTP_ADMIN               = 303_009
	// HERE HER HERE 303_010
	ERR_CREATE_TABLE_EXEC                      = 303_011
	ERR_UPDATE_TABLE_EXEC                      = 303_012
	ERR_DELETE_TABLE_EXEC                      = 303_013
	ERR_INSERT_SQLKITE_TABLES                  = 303_014
	ERR_UPDATE_SQLKITE_TABLES                  = 303_015
	ERR_DELETE_SQLKITE_TABLES                  = 303_016
	ERR_TABLE_SERIALIZE                        = 303_017
	ERR_GET_PAGE_SIZE                          = 303_018
	ERR_MAX_PAGE_COUNT                         = 303_019
	ERR_CREATE_SQLKITE_USER                    = 303_020
	ERR_CREATE_READ_TABLE_DEFINITIONS          = 303_021
	ERR_CREATE_READ_TABLE_ROWS                 = 303_022
	ERR_DB_CREATE_PATH                         = 303_023
	ERR_DB_OPEN                                = 303_024
	ERR_PRAGMA_JOURNAL                         = 303_025
	ERR_CREATE_SQLKITE_TABLES                  = 303_026
	ERR_SUPER_PG_NEW                           = 303_027
	ERR_SUPER_PG_PING                          = 303_028
	ERR_SUPER_PG_GET_PROJECT                   = 303_030
	ERR_SUPER_PG_GET_UPDATED_PROJECT_COUNT     = 303_031
	ERR_SUPER_PG_GET_UPDATED_PROJECT           = 303_032
	ERR_SUPER_SQLITE_NEW                       = 303_033
	ERR_SUPER_SQLITE_PING                      = 303_034
	ERR_SUPER_SQLITE_GET_PROJECT               = 303_035
	ERR_SUPER_SQLITE_GET_UPDATED_PROJECT_COUNT = 303_036
	ERR_SUPER_SQLITE_GET_UPDATED_PROJECT       = 303_037
	ERR_CREATE_ACCESS_TRIGGER                  = 303_038
	ERR_UPDATE_ACCESS_TRIGGER                  = 303_039
	ERR_CREATE_SQLKITE_USERS                   = 303_040
	ERR_CREATE_SQLKITE_SESSIONS                = 303_041
)
