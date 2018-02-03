package clickhouse

import (
	"database/sql/driver"
	"time"
)

// Array wraps slice or array into driver.Valuer interface to allow pass through it from database/sql
func Array(v interface{}) driver.Valuer {
	return array{v: v}
}

// Date returns date for t
func Date(t time.Time) driver.Valuer {
	return date(t)
}

type array struct {
	v interface{}
}

// Value implements driver.Valuer
func (a array) Value() (driver.Value, error) {
	return []byte(textEncode.Encode(a.v)), nil
}

type date time.Time

// Value implements driver.Valuer
func (d date) Value() (driver.Value, error) {
	return []byte(formatDate(time.Time(d))), nil
}
