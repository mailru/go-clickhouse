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
	dateFormat = "2006-01-02"
	timeFormat = "2006-01-02 15:04:05"
)

var (
	textEncode encoder = new(textEncoder)
)

type encoder interface {
	Encode(value driver.Value) string
}

type decoder interface {
	Decode(t string, value []byte) (driver.Value, error)
}

type textEncoder struct {
}

type textDecoder struct {
	location *time.Location
}

func (e *textEncoder) Encode(value driver.Value) string {
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
	case []byte:
		return string(v)
	case time.Time:
		return formatTime(v)
	}

	vv := reflect.ValueOf(value)
	switch vv.Kind() {
	case reflect.Interface, reflect.Ptr:
		return e.Encode(vv.Elem().Interface())
	case reflect.Slice, reflect.Array:
		res := "["
		for i := 0; i < vv.Len(); i++ {
			if i > 0 {
				res += ","
			}
			res += e.Encode(vv.Index(i).Interface())
		}
		res += "]"
		return res
	}
	return fmt.Sprint(value)
}

func (d *textDecoder) Decode(t string, value []byte) (driver.Value, error) {
	v := string(value)
	switch t {
	case "Date":
		return time.ParseInLocation(dateFormat, unquote(v), d.location)
	case "DateTime":
		return time.ParseInLocation(timeFormat, unquote(v), d.location)
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
	if strings.HasPrefix(t, "FixedString") {
		return unescape(unquote(v)), nil
	}
	if strings.HasPrefix(t, "Array") {
		if len(v) > 0 && v[0] == '[' && v[len(value)-1] == ']' {
			items := strings.Split(v[1:len(value)-1], ",")
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
