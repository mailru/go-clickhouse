package clickhouse

import (
	"bytes"
	"io"
	"net/http"
	"reflect"
	"strings"
	"time"
)

var (
	escaper   = strings.NewReplacer(`\`, `\\`, `'`, `\'`)
	unescaper = strings.NewReplacer(`\\`, `\`, `\'`, `'`)
)

func escape(s string) string {
	return escaper.Replace(s)
}

func unescape(s string) string {
	return unescaper.Replace(s)
}

func quote(s string) string {
	return "'" + s + "'"
}

func unquote(s string) string {
	if len(s) > 0 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return s[1 : len(s)-1]
	}
	return s
}

func formatTime(value time.Time) string {
	return quote(value.Format(timeFormat))
}

func formatDate(value time.Time) string {
	return quote(value.Format(dateFormat))
}

func readResponse(response *http.Response) (result []byte, err error) {
	if response.ContentLength > 0 {
		result = make([]byte, response.ContentLength)
	}
	buf := bytes.NewBuffer(result)
	defer response.Body.Close()
	_, err = buf.ReadFrom(response.Body)
	result = buf.Bytes()
	return
}

func readHeader(src io.Reader) (colNames, colTypes []string, err error) {
	var (
		colNamesString string
		typesString    string
	)
	colNamesString, err = readStringUntil(src, '\n')
	if err != nil {
		return nil, nil, err
	}
	colNames = strings.Split(colNamesString, "\t")

	typesString, err = readStringUntil(src, '\n')
	if err != nil && err != io.EOF {
		return nil, nil, err
	}
	colTypes = strings.Split(typesString, "\t")
	if len(colNames) != len(colTypes) {
		return nil, nil, ErrMalformed
	}
	return colNames, colTypes, nil
}

func readStringUntil(reader io.Reader, delimiter byte) (string, error) {
	var (
		buf    []byte
		symbol = make([]byte, 1)
		err    error
	)

	for {
		if _, err = reader.Read(symbol); err != nil {
			return string(buf), err
		}
		if symbol[0] == delimiter {
			return string(buf), nil
		}
		buf = append(buf, symbol[0])
	}
}

func columnType(name string) reflect.Type {
	switch name {
	case "Date", "DateTime":
		return reflect.ValueOf(time.Time{}).Type()
	case "UInt8":
		return reflect.ValueOf(uint8(0)).Type()
	case "UInt16":
		return reflect.ValueOf(uint16(0)).Type()
	case "UInt32":
		return reflect.ValueOf(uint32(0)).Type()
	case "UInt64":
		return reflect.ValueOf(uint64(0)).Type()
	case "Int8":
		return reflect.ValueOf(int8(0)).Type()
	case "Int16":
		return reflect.ValueOf(int16(0)).Type()
	case "Int32":
		return reflect.ValueOf(int32(0)).Type()
	case "Int64":
		return reflect.ValueOf(int64(0)).Type()
	case "Float32":
		return reflect.ValueOf(float32(0)).Type()
	case "Float64":
		return reflect.ValueOf(float64(0)).Type()
	case "String":
		return reflect.ValueOf("").Type()
	}
	if strings.HasPrefix(name, "FixedString") {
		return reflect.ValueOf("").Type()
	}
	if strings.HasPrefix(name, "Array") {
		subType := columnType(name[6 : len(name)-1])
		if subType != nil {
			return reflect.SliceOf(subType)
		}
		return nil
	}
	if strings.HasPrefix(name, "Enum") {
		return reflect.ValueOf("").Type()
	}
	return nil
}
