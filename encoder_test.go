package clickhouse

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTextEncoder(t *testing.T) {
	dt := time.Date(2011, 3, 6, 6, 20, 0, 0, time.UTC)
	d := time.Date(2012, 5, 31, 0, 0, 0, 0, time.UTC)
	testCases := []struct {
		value    interface{}
		expected string
	}{
		{true, "1"},
		{int8(1), "1"},
		{int16(1), "1"},
		{int32(1), "1"},
		{int64(1), "1"},
		{int(-1), "-1"},
		{uint8(1), "1"},
		{uint16(1), "1"},
		{uint32(1), "1"},
		{uint64(1), "1"},
		{uint(1), "1"},
		{float32(1), "1"},
		{float64(1), "1"},
		{dt, "'2011-03-06 06:20:00'"},
		{d, "'2012-05-31 00:00:00'"},
		{"hello", "'hello'"},
		{[]byte("hello"), "hello"},
		{`\\'hello`, `'\\\\\'hello'`},
		{[]byte(`\\'hello`), `\\'hello`},
		{[]int32{1, 2}, "[1,2]"},
		{[]int32{}, "[]"},
		{Array([]int8{1}), "[1]"},
		{Array([]interface{}{Array([]int8{1})}), "[[1]]"},
		{[][]int16{{1}}, "[[1]]"},
		{[]int16(nil), "[]"},
		{(*int16)(nil), "NULL"},
	}

	enc := new(textEncoder)
	for _, tc := range testCases {
		v, err := enc.Encode(tc.value)
		if assert.NoError(t, err) {
			assert.Equal(t, tc.expected, string(v))
		}
	}
}
