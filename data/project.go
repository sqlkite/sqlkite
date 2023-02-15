package data

type Project struct {
	Id                   string `json:"id"`
	Debug                bool   `json:"debug"`
	MaxConcurrency       uint16 `json:"max_concurrency"`
	MaxSQLLength         uint32 `json:"max_sql_length"`
	MaxSQLParameterCount uint16 `json:"max_sql_parameter_count"`
	MaxDatabaseSize      uint64 `json:"max_database_size"`
	MaxResultLength      uint32 `json:"max_result_length"`
	MaxFromCount         uint16 `json:"max_from_count"`
	MaxSelectColumnCount uint16 `json:"max_select_column_count"`
	MaxConditionCount    uint16 `json:"max_condition_count"`
	MaxOrderByCount      uint16 `json:"max_order_by_count"`
	MaxTableCount        uint16 `json:"max_table_count"`

	// Tables can have their own value which will be <= this
	// They can also have a MaxDeleteCount and MaxUpdateCount which can interact
	// with MaxSelectCount in the case where the update or delete has a "returning"
	MaxSelectCount uint16 `json:"max_select_count"`
}
