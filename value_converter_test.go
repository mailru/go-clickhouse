package clickhouse

import (
	"database/sql/driver"
	"math"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func getUInt64Ptr(v uint64) *uint64 {
	return &v
}

func TestConverter(t *testing.T) {
	testCases := []struct {
		value    interface{}
		expected driver.Value
		msg      string
	}{
		// uint64
		{getUInt64Ptr(0), uint64(0), "*uint64(0)"},
		{getUInt64Ptr(maxAllowedUInt64), uint64(9223372036854775807), "*uint64(maxAllowedUInt64)"},
		{getUInt64Ptr(maxAllowedUInt64 + 1), []byte("9223372036854775808"), "*uint64(maxAllowedUInt64+1)"},
		{getUInt64Ptr(maxAllowedUInt64*2 + 1), []byte("18446744073709551615"), "*uint64(maxUInt64)"},

		// int64
		{int64(0), int64(0), "int64(0)"},
		{int64(math.MinInt64), int64(-9223372036854775808), "int64(MinInt64)"},
		{int64(math.MaxInt64), int64(9223372036854775807), "int64(MaxInt64)"},
		// uint64
		{uint64(0), uint64(0), "uint64(0)"},
		{uint64(maxAllowedUInt64), uint64(9223372036854775807), "uint64(maxAllowedUInt64)"},
		{uint64(maxAllowedUInt64 + 1), []byte("9223372036854775808"), "uint64(maxAllowedUInt64+1)"},
		{uint64(maxAllowedUInt64*2 + 1), []byte("18446744073709551615"), "uint64(maxUInt64)"},
	}

	for _, tc := range testCases {
		dv, err := converter{}.ConvertValue(tc.value)
		if assert.NoError(t, err) {
			// assert.ElementsMatch(t, dv, tc.expected, "failed to convert "+tc.msg)
			if !reflect.DeepEqual(tc.expected, dv) {
				t.Errorf("failed to convert %s", tc.msg)
			}
		}
	}
}
