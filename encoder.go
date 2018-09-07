package clickhouse

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const (
	dateFormat     = "2006-01-02"
	timeFormat     = "2006-01-02 15:04:05"
	timeZoneBorder = "\\'"
)

var (
	textEncode encoder = new(textEncoder)
)

type encoder interface {
	Encode(value driver.Value) ([]byte, error)
}

type decoder interface {
	Decode(t string, value []byte) (driver.Value, error)
}

type textEncoder struct {
}

type textDecoder struct {
	location      *time.Location
	useDBLocation bool
}

// Encode encodes driver value into string
// Note: there is 2 convention:
// type string will be quoted
// type []byte will be encoded as is (raw string)
func (e *textEncoder) Encode(value driver.Value) ([]byte, error) {
	switch v := value.(type) {
	case array:
		return e.encodeArray(reflect.ValueOf(v.v))
	case []byte:
		return v, nil
	}

	vv := reflect.ValueOf(value)
	switch vv.Kind() {
	case reflect.Interface, reflect.Ptr:
		if vv.IsNil() {
			return []byte("NULL"), nil
		}
		return e.Encode(vv.Elem().Interface())
	case reflect.Slice, reflect.Array:
		return e.encodeArray(vv)
	}
	return []byte(e.encode(value)), nil
}

func (e *textEncoder) encode(value driver.Value) string {
	if value == nil {
		return "NULL"
	}
	switch v := value.(type) {
	case bool:
		if v {
			return "1"
		}
		return "0"
	case int:
		return strconv.FormatInt(int64(v), 10)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case string:
		return quote(escape(v))
	case time.Time:
		return formatTime(v)
	}

	return fmt.Sprint(value)
}

// EncodeArray encodes a go slice or array as Clickhouse Array
func (e *textEncoder) encodeArray(value reflect.Value) ([]byte, error) {
	if value.Kind() != reflect.Slice && value.Kind() != reflect.Array {
		return nil, fmt.Errorf("expected array or slice, got %s", value.Kind())
	}

	res := make([]byte, 0)
	res = append(res, '[')
	for i := 0; i < value.Len(); i++ {
		if i > 0 {
			res = append(res, ',')
		}
		tmp, err := e.Encode(value.Index(i).Interface())
		if err != nil {
			return nil, err
		}
		res = append(res, tmp...)
	}
	return append(res, ']'), nil
}

func (d *textDecoder) Decode(t string, value []byte) (driver.Value, error) {
	v := string(value)
	switch t {
	case "Date":
		uv := unquote(v)
		if uv == "0000-00-00" {
			return time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC), nil
		}
		return time.ParseInLocation(dateFormat, uv, d.location)
	case "DateTime":
		uv := unquote(v)
		if uv == "0000-00-00 00:00:00" {
			return time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC), nil
		}
		return time.ParseInLocation(timeFormat, uv, d.location)
	case "UInt8":
		vv, err := strconv.ParseUint(v, 10, 8)
		return uint8(vv), err
	case "UInt16":
		vv, err := strconv.ParseUint(v, 10, 16)
		return uint16(vv), err
	case "UInt32":
		vv, err := strconv.ParseUint(v, 10, 32)
		return uint32(vv), err
	case "UInt64":
		return strconv.ParseUint(v, 10, 64)
	case "Int8":
		vv, err := strconv.ParseInt(v, 10, 8)
		return int8(vv), err
	case "Int16":
		vv, err := strconv.ParseInt(v, 10, 16)
		return int16(vv), err
	case "Int32":
		vv, err := strconv.ParseInt(v, 10, 32)
		return int32(vv), err
	case "Int64":
		return strconv.ParseInt(v, 10, 64)
	case "Float32":
		vv, err := strconv.ParseFloat(v, 64)
		return float32(vv), err
	case "Float64":
		return strconv.ParseFloat(v, 64)
	case "String":
		return unescape(unquote(v)), nil
	}

	// got zoned datetime
	if strings.HasPrefix(t, "DateTime") {
		var (
			loc *time.Location
			err error
		)

		if d.useDBLocation {
			left := strings.Index(t, timeZoneBorder)
			if left == -1 {
				return nil, fmt.Errorf("time zone not found")
			}
			right := strings.LastIndex(t, timeZoneBorder)
			timeZoneName := t[left+len(timeZoneBorder) : right]

			loc, err = time.LoadLocation(timeZoneName)
			if err != nil {
				return nil, err
			}
		} else {
			loc = d.location
		}

		var t time.Time
		if t, err = time.ParseInLocation(timeFormat, unquote(v), loc); err != nil {
			return t, err
		}
		return t.In(d.location), nil
	}

	if strings.HasPrefix(t, "FixedString") {
		return unescape(unquote(v)), nil
	}
	if strings.HasPrefix(t, "Array") {
		if len(v) > 0 && v[0] == '[' && v[len(v)-1] == ']' {
			var items []string
			// check if array is string encoded (['example'])
			if len(v) > 4 && v[1] == '\'' && v[len(v)-2] == '\'' {
				items = strings.Split(v[2:len(v)-2], "','")
			} else if len(v) > 2 { // check that array is not empty ([])
				items = strings.Split(v[1:len(v)-1], ",")
			}

			subType := t[6 : len(t)-1]
			r := reflect.MakeSlice(reflect.SliceOf(columnType(subType)), len(items), len(items))
			for i, item := range items {
				vv, err := d.Decode(subType, []byte(item))
				if err != nil {
					return nil, err
				}
				r.Index(i).Set(reflect.ValueOf(vv))
			}
			return r.Interface(), nil
		}
		return nil, ErrMalformed
	}
	if strings.HasPrefix(t, "Enum") {
		return unquote(v), nil
	}
	return value, nil
}
