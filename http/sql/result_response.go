package sql

import (
	"errors"
	"io"
	"strconv"

	"src.goblgobl.com/utils/log"

	"github.com/valyala/fasthttp"
	"src.goblgobl.com/sqlkite"
	"src.goblgobl.com/utils/buffer"
	"src.goblgobl.com/utils/http"
)

func NewResultResponse(result *sqlkite.QueryResult) http.Response {
	buffer := result.Result
	rowCount := result.RowCount
	if buffer == nil {
		return AffectedResponse{rowCount}
	}

	if rowCount == 0 {
		return EmptyResultResponse{}
	}

	// ignore error, whatever called us should have handled any buffer errors already
	data, _ := buffer.Bytes()
	return &ResultResponse{
		data:     data,
		buffer:   buffer,
		rowCount: rowCount,
		len:      buffer.Len() + SELECT_ENVELOPE_FIXED_LENGTH,
	}
}

type ResultResponse struct {
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

	rowCount int
}

// io.Closer
func (r *ResultResponse) Close() error {
	r.buffer.Release()
	return nil
}

// io.Reader
func (r *ResultResponse) Read(p []byte) (int, error) {
	// we expect WriteTo to always be used
	// we only implement this to satisfy SetBodyStream which requires
	// and io.Reader, even though it won't use it.
	panic(errors.New("select_resonse.read"))
}

// io.WriterTo
func (r *ResultResponse) WriteTo(w io.Writer) (int64, error) {
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

func (r *ResultResponse) Write(conn *fasthttp.RequestCtx, logger log.Logger) log.Logger {
	conn.SetStatusCode(200)
	conn.SetBodyStream(r, r.len)
	return logger.Field(http.OkLogData).Int("res", r.len).Int("rows", r.rowCount)
}

type AffectedResponse struct {
	Affected int
}

func (r AffectedResponse) Write(conn *fasthttp.RequestCtx, logger log.Logger) log.Logger {
	affected := r.Affected
	body := []byte(`{"affected":` + strconv.Itoa(affected) + "}")

	conn.SetStatusCode(200)
	conn.SetBody(body)
	return logger.Field(http.OkLogData).Int("res", len(body)).Int("affected", affected)
}

type EmptyResultResponse struct {
}

func (r EmptyResultResponse) Write(conn *fasthttp.RequestCtx, logger log.Logger) log.Logger {
	conn.SetStatusCode(200)
	body := []byte(`{"r":[]}`)
	conn.SetBody(body)
	return logger.Field(http.OkLogData).Int("res", len(body)).Int("rows", 0)
}
