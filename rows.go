package clickhouse

import (
	"database/sql/driver"
	"encoding/csv"
	"io"
	"reflect"
	"time"
)

func newTextRows(c *conn, body io.ReadCloser, location *time.Location, useDBLocation bool) (*textRows, error) {
	tsvReader := csv.NewReader(body)
	tsvReader.Comma = '\t'

	columns, err := tsvReader.Read()
	if err != nil {
		return nil, err
	}

	types, err := tsvReader.Read()
	if err != nil {
		return nil, err
	}

	return &textRows{
		c:        c,
		respBody: body,
		tsv:      tsvReader,
		columns:  columns,
		types:    types,
		decode:   &textDecoder{location: location, useDBLocation: useDBLocation},
	}, nil
}

type textRows struct {
	c        *conn
	respBody io.ReadCloser
	tsv      *csv.Reader
	columns  []string
	types    []string
	decode   decoder
}

func (r *textRows) Columns() []string {
	return r.columns
}

func (r *textRows) Close() error {
	r.c.cancel = nil
	return r.respBody.Close()
}

func (r *textRows) Next(dest []driver.Value) error {
	row, err := r.tsv.Read()
	if err != nil {
		return err
	}

	for i, s := range row {
		v, err := r.decode.Decode(r.types[i], []byte(s))
		if err != nil {
			return err
		}
		dest[i] = v
	}

	return nil
}

// ColumnTypeScanType implements the driver.RowsColumnTypeScanType
func (r *textRows) ColumnTypeScanType(index int) reflect.Type {
	return columnType(r.types[index])
}

// ColumnTypeDatabaseTypeName implements the driver.RowsColumnTypeDatabaseTypeName
func (r *textRows) ColumnTypeDatabaseTypeName(index int) string {
	return r.types[index]
}
