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
	VAL_INVALID_SELECT_FROM          = 301_012
	VAL_INVALID_SELECT_FROM_COUNT    = 301_013
	VAL_INVALID_SELECT_FROM_TABLE    = 301_014
	VAL_INVALID_SELECT_FROM_JOIN     = 301_015
	VAL_INVALID_SELECT_FROM_ON       = 301_016
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

	RES_UNKNOWN_ROUTE          = 302_001
	RES_MISSING_PROJECT_HEADER = 302_002
	RES_MISSING_SUBDOMAIN      = 302_003
	RES_PROJECT_NOT_FOUND      = 302_004
	RES_DATABASE_ERROR         = 302_005

	ERR_READ_CONFIG                     = 303_001
	ERR_PARSE_CONFIG                    = 303_002
	ERR_INVALID_STORAGE_TYPE            = 303_003
	ERR_CONFIG_ROOT_PATH_REQUIRED       = 303_004
	ERR_CONFIG_ROOT_PATH_INVALID        = 303_005
	ERR_CONFIG_ROOT_PATH_FILE           = 303_006
	ERR_CONFIG_HTTP_PROJECT_TYPE        = 303_007
	ERR_CONFIG_HTTP_AUTHENTICATION_TYPE = 303_008
	ERR_CONFIG_HTTP_ADMIN               = 303_009
)
