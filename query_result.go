package sqlkite

import "src.goblgobl.com/utils/buffer"

type QueryResult struct {
	Result   *buffer.Buffer
	RowCount int
}
