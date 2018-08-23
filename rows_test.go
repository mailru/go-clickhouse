package clickhouse

import (
	"bytes"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func newBytesReaderCloser(data []byte) *bytesReaderCloser {
	return &bytesReaderCloser{bytes.NewReader(data)}
}

type bytesReaderCloser struct {
	*bytes.Reader
}

func (rc *bytesReaderCloser) Close() error {
	return nil
}

func TestTextRowsOk(t *testing.T) {
	data := []byte("Number\tText\nInt32\tString\n1\t'hello'\n2\t'world'\n")
	rows, err := newTextRows(newBytesReaderCloser(data), 8, time.Local, false)
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, []string{"Number", "Text"}, rows.Columns())
	assert.Equal(t, []string{"Int32", "String"}, rows.types)
	assert.Equal(t, reflect.TypeOf(int32(0)), rows.ColumnTypeScanType(0))
	assert.Equal(t, reflect.TypeOf(""), rows.ColumnTypeScanType(1))
	assert.Equal(t, "Int32", rows.ColumnTypeDatabaseTypeName(0))
	assert.Equal(t, "String", rows.ColumnTypeDatabaseTypeName(1))

	assert.Equal(t, time.Local, rows.decode.(*textDecoder).location)
	dest := make([]driver.Value, 2)
	if !assert.NoError(t, rows.Next(dest)) {
		return
	}
	assert.Equal(t, []driver.Value{int32(1), "hello"}, dest)
	if !assert.NoError(t, rows.Next(dest)) {
		return
	}
	assert.Equal(t, []driver.Value{int32(2), "world"}, dest)
	assert.Equal(t, io.EOF, rows.Next(dest))
	assert.NoError(t, rows.Close())
}

func TestTextRowsTooSmallBuffer(t *testing.T) {
	data := []byte("Number\tText\nInt32\tString\n1\t'hello'\n")
	rows, err := newTextRows(newBytesReaderCloser(data), 4, time.Local, false)
	if !assert.NoError(t, err) {
		return
	}
	dest := make([]driver.Value, 2)
	assert.Equal(t, errors.New("failed to fit column to buffer"), rows.Next(dest))
	assert.NoError(t, rows.Close())
}
