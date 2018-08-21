package clickhouse

import (
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"time"
)

type textRows struct {
	columns       []string
	types         []string
	src           io.ReadCloser
	decode        decoder
	buffer        []byte
	prevReadCnt   int
	unparsedStart int
	unparsedEnd   int
}

// Columns returns the columns names
func (r *textRows) Columns() []string {
	return r.columns
}

// ColumnTypeScanType implements the driver.RowsColumnTypeScanType
func (r *textRows) ColumnTypeScanType(index int) reflect.Type {
	return columnType(r.types[index])
}

// ColumnTypeDatabaseTypeName implements the driver.RowsColumnTypeDatabaseTypeName
func (r *textRows) ColumnTypeDatabaseTypeName(index int) string {
	return r.types[index]
}

// Close closes the rows iterator.
func (r *textRows) Close() error {
	return r.src.Close()
}

// Next is called to populate the next row of data into
func (r *textRows) Next(dest []driver.Value) error {
	var (
		readErr                                    error
		err                                        error
		lineFinished, needMoreData                 bool
		bytesRead, valuesParsed, totalValuesParsed int
	)

	isEmptyBuffer := r.unparsedStart == 0 && r.unparsedEnd == 0
	for {
		if needMoreData || isEmptyBuffer {
			bytesRead, readErr = r.src.Read(r.buffer[r.prevReadCnt:]) // do not overwrite tail of the previous line
			if readErr != nil && readErr != io.EOF {
				return readErr
			} else if readErr == io.EOF && bytesRead == 0 {
				return io.EOF
			}
			r.unparsedEnd = r.prevReadCnt + bytesRead
		}

		valuesParsed, needMoreData, lineFinished, err = r.readIn(dest, totalValuesParsed)
		totalValuesParsed += valuesParsed
		if err != nil {
			return err
		}
		if valuesParsed == 0 && r.unparsedEnd-r.unparsedStart == len(r.buffer) {
			return errors.New("failed to fit column to buffer")
		}
		if lineFinished {
			return nil
		}

		if needMoreData {
			r.prevReadCnt = r.unparsedEnd - r.unparsedStart
			for i, j := r.unparsedStart, 0; i != r.unparsedEnd; i, j = i+1, j+1 { // copy rest of the previous line to the start of the buffer
				r.buffer[j] = r.buffer[i]
			}
			r.unparsedStart = 0
			r.unparsedEnd = r.prevReadCnt
		}
	}
}

func (r *textRows) readIn(dest []driver.Value, startValID int) (valuesRead int, needMoreData, lineFinished bool, err error) {
	src := r.buffer[r.unparsedStart:r.unparsedEnd]
	valStart := r.unparsedStart
	indOffset := valStart
	valEnd := valStart
	maxValEnd := indOffset + len(src)
	valID := startValID
	var ch byte

	for valEnd != maxValEnd {
		ch = src[valEnd-indOffset]
		switch ch {
		case '\t':
			dest[valID], err = r.decode.Decode(r.types[valID], r.buffer[valStart:valEnd])
			if err != nil {
				return 0, false, false, err
			}
			valID++
			valEnd++
			r.unparsedStart += valEnd - valStart
			valStart = valEnd
		case '\n':
			if valEnd == valStart {
				// totals are separated by empty line
				valEnd++
				valStart = valEnd
				continue
			}
			dest[valID], err = r.decode.Decode(r.types[valID], r.buffer[valStart:valEnd])
			r.unparsedStart += valEnd - valStart + 1

			valID++
			return valID - startValID, false, true, nil
		default:
			valEnd++
		}
	}
	return valID - startValID, true, false, nil
}

func newTextRows(src io.ReadCloser, buffsize int64, location *time.Location, useDBLocation bool) (*textRows, error) {
	columns, types, err := readHeader(src)
	if err != nil {
		return nil, err
	}
	return &textRows{columns: columns, types: types, src: src, buffer: make([]byte, buffsize), decode: &textDecoder{location: location, useDBLocation: useDBLocation}}, nil
}
