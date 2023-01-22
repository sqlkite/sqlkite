package data

type Project struct {
	Id                   string `json:"id"`
	MaxConcurrency       uint16 `json:"max_concurrency"`
	MaxSQLLength         uint32 `json:"max_sql_length"`
	MaxSQLParameterCount uint16 `json:"max_sql_parameters_count"`
	MaxDatabaseSize      uint64 `json:"max_database_size"`
	MaxRowCount          uint16 `json:"max_row_count"`
	MaxResultLength      uint32 `json:"max_result_length"`
	MaxFromCount         uint16 `json:"max_from_count"`
	MaxSelectColumnCount uint16 `json:"max_select_column_count"`
	MaxConditionCount    uint16 `json:"max_condition_count"`
	MaxOrderByCount      uint16 `json:"max_order_by_count"`
}
