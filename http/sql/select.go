package sql

import (
	"errors"
	"fmt"
	"io"

	"github.com/valyala/fasthttp"
	"src.goblgobl.com/sqlkite"
	"src.goblgobl.com/sqlkite/codes"
	"src.goblgobl.com/sqlkite/http/sql/parser"
	"src.goblgobl.com/sqlkite/sql"
	"src.goblgobl.com/utils/buffer"
	"src.goblgobl.com/utils/http"
	"src.goblgobl.com/utils/log"
	"src.goblgobl.com/utils/optional"
	"src.goblgobl.com/utils/typed"
	"src.goblgobl.com/utils/validation"
)

var (
	SELECT_ENVELOPE_START        = []byte(`{"r":[`)
	SELECT_ENVELOPE_END          = []byte(`]}`)
	SELECT_ENVELOPE_FIXED_LENGTH = len(SELECT_ENVELOPE_START) + len(SELECT_ENVELOPE_END)
)

func Select(conn *fasthttp.RequestCtx, env *sqlkite.Env) (http.Response, error) {
	input, err := typed.Json(conn.PostBody())
	if err != nil {
		return http.InvalidJSON, nil
	}

	project := env.Project
	validator := env.Validator

	columns := selectParseColumns(input[SELECT_INPUT_NAME], validator, project)
	froms := selectParseFrom(input[FROM_INPUT_NAME], validator, project)
	where := selectParseWhere(input[WHERE_INPUT_NAME], validator)
	orderBy := selectParseOrderBy(input[ORDER_INPUT_NAME], validator, project)
	limit := selectParseLimit(input[LIMIT_INPUT_NAME], validator, project)
	offset := selectParseOffset(input[OFFSET_INPUT_NAME], validator)
	parameters := extractParameters(input[PARAMETERS_INPUT_NAME], validator, project)

	// There's more validation to do, and we do like to return all errors in one
	// shot, but it's possible trying to go further will just cause more problems.
	if !validator.IsValid() {
		return http.Validation(validator), nil
	}

	sel := sql.Select{
		Columns:    columns,
		Froms:      froms,
		Where:      where,
		Parameters: parameters,
		Limit:      limit,
		Offset:     offset,
		OrderBy:    orderBy,
	}

	result, err := project.Select(env, sel)
	if err != nil {
		return handleError(env, err)
	}

	if !validator.IsValid() {
		return http.Validation(validator), nil
	}

	return NewSelectResponse(result), nil
}

func selectParseColumns(input any, validator *validation.Result, p *sqlkite.Project) []sql.DataField {
	if input == nil {
		validator.AddInvalidField(selectField, valRequired)
		return nil
	}

	rawColumns, ok := input.([]any)
	if !ok {
		validator.AddInvalidField(selectField, valArrayType)
		return nil
	}

	max := int(p.MaxSelectColumnCount)
	if len(rawColumns) > max {
		validator.AddInvalidField(selectField, validation.Invalid{
			Code:  codes.VAL_SQL_TOO_MANY_SELECT,
			Error: fmt.Sprintf("must return no more than %d columns", max),
			Data:  validation.Max(max),
		})
		return nil
	}

	validator.BeginArray()
	columns := make([]sql.DataField, len(rawColumns))
	for i, rawColumn := range rawColumns {
		column, err := parser.SelectColumn(rawColumn)
		if err != nil {
			validator.ArrayIndex(i)
			validator.AddInvalidField(selectField, *err)
		} else {
			columns[i] = column
		}
	}
	validator.EndArray()

	return columns
}

func selectParseFrom(input any, validator *validation.Result, p *sqlkite.Project) []sql.SelectFrom {
	if input == nil {
		validator.AddInvalidField(fromField, valRequired)
		return nil
	}

	rawFroms, ok := input.([]any)
	if !ok {
		validator.AddInvalidField(fromField, valArrayType)
		return nil
	}

	max := int(p.MaxFromCount)
	if len(rawFroms) > max {
		validator.AddInvalidField(fromField, validation.Invalid{
			Code:  codes.VAL_TOO_MANY_FROMS,
			Error: fmt.Sprintf("must have no more than %d froms", max),
			Data:  validation.Max(max),
		})
		return nil
	}

	validator.BeginArray()
	froms := make([]sql.SelectFrom, len(rawFroms))
	for i, rawFrom := range rawFroms {
		from, err := parser.SelectFrom(rawFrom)
		if err != nil {
			validator.ArrayIndex(i)
			validator.AddInvalidField(fromField, *err)
		} else {
			froms[i] = from
		}
	}
	validator.EndArray()
	return froms
}

func selectParseWhere(input any, validator *validation.Result) sql.Condition {
	if input == nil {
		return sql.EmptyCondition
	}

	rawWhere, ok := input.([]any)
	if !ok {
		validator.AddInvalidField(whereField, valArrayType)
		return sql.EmptyCondition
	}

	condition, err := parser.Condition(rawWhere, 0)
	if err != nil {
		validator.AddInvalidField(whereField, *err)
	}
	return condition
}

func selectParseOrderBy(input any, validator *validation.Result, p *sqlkite.Project) []sql.OrderBy {
	if input == nil {
		return nil
	}

	rawOrderBy, ok := input.([]any)
	if !ok {
		validator.AddInvalidField(orderField, valArrayType)
		return nil
	}

	max := int(p.MaxOrderByCount)
	if len(rawOrderBy) > max {
		validator.AddInvalidField(orderField, validation.Invalid{
			Code:  codes.VAL_SQL_TOO_MANY_ORDER_BY,
			Error: fmt.Sprintf("must have no more than %d ordering columns", max),
			Data:  validation.Max(max),
		})
		return nil
	}

	validator.BeginArray()
	fields := make([]sql.OrderBy, len(rawOrderBy))
	for i, rawOrderBy := range rawOrderBy {
		orderBy, err := parser.OrderBy(rawOrderBy)
		if err != nil {
			validator.ArrayIndex(i)
			validator.AddInvalidField(orderField, *err)
		} else {
			fields[i] = orderBy
		}
	}
	validator.EndArray()
	return fields
}

func selectParseLimit(input any, validator *validation.Result, p *sqlkite.Project) int {
	max := int(p.MaxRowCount)
	if input == nil {
		return max
	}

	limit, ok := input.(float64)
	if !ok {
		validator.AddInvalidField(limitField, valIntType)
		return 0
	}

	n := int(limit)
	if n > max {
		validator.AddInvalidField(limitField, validation.Invalid{
			Code:  codes.VAL_SQL_LIMIT_TOO_HIGH,
			Error: fmt.Sprintf("limit cannot exceed %d", max),
			Data:  validation.Max(max),
		})
		return 0
	}

	return n
}

func selectParseOffset(input any, validator *validation.Result) optional.Value[int] {
	if input == nil {
		return optional.NullInt
	}

	offset, ok := input.(float64)
	if !ok {
		validator.AddInvalidField(offsetField, valIntType)
		return optional.NullInt
	}

	return optional.Int(int(offset))
}

func NewSelectResponse(result *sqlkite.SelectResult) *SelectResponse {
	buffer := result.Result
	// ignore error, whatever called us should have handled any buffer errors already
	data, _ := buffer.Bytes()
	return &SelectResponse{
		data:   data,
		buffer: buffer,
		len:    buffer.Len() + SELECT_ENVELOPE_FIXED_LENGTH,
	}
}

type SelectResponse struct {
	// how much of the buffer we've read
	read int

	// total length of the response we plan on sending out
	// this is len(data) + json envelope
	len int

	// buffer.Bytes()
	data []byte

	// the buffer containing the result data, we need to hold on to this
	// since we now own it and are responsible for releasing it.
	buffer *buffer.Buffer
}

// io.Closer
func (r *SelectResponse) Close() error {
	r.buffer.Release()
	return nil
}

// io.Reader
func (r *SelectResponse) Read(p []byte) (int, error) {
	// we expect WriteTo to always be used
	// we only implement this to satisfy SetBodyStream which requires
	// and io.Reader, even though it won't use it.
	panic(errors.New("select_resonse.read"))
}

// io.WriterTo
func (r *SelectResponse) WriteTo(w io.Writer) (int64, error) {
	n1, err := w.Write(SELECT_ENVELOPE_START)
	written := int64(n1)
	if err != nil {
		return written, err
	}

	n2, err := w.Write(r.data)
	written += int64(n2)
	if err != nil {
		return written, err
	}

	n3, err := w.Write(SELECT_ENVELOPE_END)
	written += int64(n3)
	if err != nil {
		return written, err
	}

	return written, err
}

func (r *SelectResponse) Write(conn *fasthttp.RequestCtx, logger log.Logger) log.Logger {
	conn.SetStatusCode(200)
	conn.SetBodyStream(r, r.len)
	return logger.Field(http.OkLogData).Int("res", r.len)
}
