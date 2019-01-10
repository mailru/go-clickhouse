package clickhouse

import (
	"bytes"
	"database/sql/driver"
	"io"
	"io/ioutil"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type bufReadCloser struct {
	*bytes.Reader
}

func (r *bufReadCloser) Close() error {
	return nil
}

func TestTextRows(t *testing.T) {
	buf := bytes.NewReader([]byte("Number\tText\nInt32\tString\n1\thello\n2\tworld\n"))
	rows, err := newTextRows(&conn{}, &bufReadCloser{buf}, time.Local, false)
	if assert.Nil(t, err) {
		return
	}
	assert.Equal(t, []string{"Number", "Text"}, rows.Columns())
	assert.Equal(t, []string{"Int32", "String"}, rows.types)
	assert.Equal(t, reflect.TypeOf(int32(0)), rows.ColumnTypeScanType(0))
	assert.Equal(t, reflect.TypeOf(""), rows.ColumnTypeScanType(1))
	assert.Equal(t, "Int32", rows.ColumnTypeDatabaseTypeName(0))
	assert.Equal(t, "String", rows.ColumnTypeDatabaseTypeName(1))

	dest := make([]driver.Value, 2)
	if assert.Nil(t, rows.Next(dest)) {
		return
	}
	assert.Equal(t, []driver.Value{int32(1), "hello"}, dest)
	if assert.Nil(t, rows.Next(dest)) {
		return
	}
	assert.Equal(t, []driver.Value{int32(2), "world"}, dest)
	data, err := ioutil.ReadAll(rows.respBody)
	if assert.Nil(t, err) {
		return
	}

	assert.Equal(t, 0, len(data))
	assert.Equal(t, io.EOF, rows.Next(dest))
	assert.NoError(t, rows.Close())
	assert.Empty(t, data)
}
