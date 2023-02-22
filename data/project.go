package data

type Project struct {
	Id     string `json:"id"`
	Auth   Auth   `json:"auth"`
	Limits Limits `json:"limits"`
	Debug  bool   `json:"debug"`
}

type Limits struct {
	MaxDatabaseSize      uint64 `json:"max_database_size"`
	MaxResultLength      uint32 `json:"max_result_length"`
	MaxSQLLength         uint32 `json:"max_sql_length"`
	MaxSelectCount       uint16 `json:"max_select_count"`
	MaxConcurrency       uint16 `json:"max_concurrency"`
	MaxSQLParameterCount uint16 `json:"max_sql_parameter_count"`
	MaxFromCount         uint16 `json:"max_from_count"`
	MaxSelectColumnCount uint16 `json:"max_select_column_count"`
	MaxConditionCount    uint16 `json:"max_condition_count"`
	MaxOrderByCount      uint16 `json:"max_order_by_count"`
	MaxTableCount        uint16 `json:"max_table_count"`
}

type Auth struct {
	Disabled bool        `json:"disabled"`
	Session  AuthSession `json:"session"`
}

type AuthSession struct {
	SessionTTL int `json:"session_ttl"`
}
