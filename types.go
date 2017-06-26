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
func Date(t time.Time) time.Time {
	y, m, d := t.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, t.Location())
}

type array struct {
	v interface{}
}

// Value implements driver.Valuer
func (a array) Value() (driver.Value, error) {
	return []byte(textEncode.Encode(a.v)), nil
}
