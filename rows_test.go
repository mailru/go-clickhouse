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
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, []string{"Number", "Text"}, rows.Columns())
	assert.Equal(t, []string{"Int32", "String"}, rows.types)
	assert.Equal(t, reflect.TypeOf(int32(0)), rows.ColumnTypeScanType(0))
	assert.Equal(t, reflect.TypeOf(""), rows.ColumnTypeScanType(1))
	assert.Equal(t, "Int32", rows.ColumnTypeDatabaseTypeName(0))
	assert.Equal(t, "String", rows.ColumnTypeDatabaseTypeName(1))

	dest := make([]driver.Value, 2)
	if !assert.NoError(t, rows.Next(dest)) {
		return
	}
	assert.Equal(t, []driver.Value{int32(1), "hello"}, dest)
	if !assert.NoError(t, rows.Next(dest)) {
		return
	}
	assert.Equal(t, []driver.Value{int32(2), "world"}, dest)
	data, err := ioutil.ReadAll(rows.respBody)
	if !assert.NoError(t, err) {
		return
	}

	assert.Equal(t, 0, len(data))
	assert.Equal(t, io.EOF, rows.Next(dest))
	assert.NoError(t, rows.Close())
	assert.Empty(t, data)
}

func TestTextRowsQuoted(t *testing.T) {
	buf := bytes.NewReader([]byte("text\nArray(String)\n['Quote: \"here\"']"))
	rows, err := newTextRows(&conn{}, &bufReadCloser{buf}, time.Local, false)
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, []string{"text"}, rows.Columns())
	assert.Equal(t, []string{"Array(String)"}, rows.types)
	dest := make([]driver.Value, 1)
	if !assert.NoError(t, rows.Next(dest)) {
		return
	}
	assert.Equal(t, []driver.Value{[]string{"Quote: \"here\""}}, dest)
}

func TestTextRowsNewLine(t *testing.T) {
	buf := bytes.NewReader([]byte("text\nString\nHello\\nThere"))
	rows, err := newTextRows(&conn{}, &bufReadCloser{buf}, time.Local, false)
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, []string{"text"}, rows.Columns())
	assert.Equal(t, []string{"String"}, rows.types)
	dest := make([]driver.Value, 1)
	if !assert.NoError(t, rows.Next(dest)) {
		return
	}
	assert.Equal(t, []driver.Value{"Hello\nThere"}, dest)
}
