package sqlkite

import "src.goblgobl.com/utils/buffer"

type MutateResultStatus int
type SelectResultStatus int

const (
	MUTATE_RESULT_OK MutateResultStatus = iota
	MUTATE_RESULT_INVALID
	MUTATE_RESULT_ACCESS_DENIED
)

type MutateResult struct {
	Status       MutateResultStatus
	RowsAffected int
}

type SelectResult struct {
	Result   *buffer.Buffer
	RowCount int
}
