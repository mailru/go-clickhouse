package clickhouse

import (
	"time"

	"testing"

	"database/sql/driver"
	"github.com/stretchr/testify/assert"
)

func TestArray(t *testing.T) {
	testCases := []struct {
		value    interface{}
		expected driver.Value
	}{
		{[]int16{1, 2}, []byte("[1,2]")},
		{[]int32{1, 2}, []byte("[1,2]")},
		{[]int64{1, 2}, []byte("[1,2]")},
		{[]uint16{1, 2}, []byte("[1,2]")},
		{[]uint32{1, 2}, []byte("[1,2]")},
		{[]uint64{1, 2}, []byte("[1,2]")},
	}

	for _, tc := range testCases {
		dv, err := Array(tc.value).Value()
		if assert.NoError(t, err) {
			assert.Equal(t, tc.expected, dv)
		}
	}
}

func TestDate(t *testing.T) {
	dt := time.Date(2016, 4, 4, 11, 22, 33, 0, time.Local)
	d := time.Date(2016, 4, 4, 0, 0, 0, 0, time.Local)
	assert.Equal(t, d, Date(dt))
}
