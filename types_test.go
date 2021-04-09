package clickhouse

import (
	"database/sql/driver"
	"net"
	"testing"
	"time"

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
		{[]uint64{}, []byte("[]")},
	}

	for _, tc := range testCases {
		dv, err := Array(tc.value).Value()
		if assert.NoError(t, err) {
			assert.Equal(t, tc.expected, dv)
		}
	}
}

func TestDate(t *testing.T) {
	d := time.Date(2016, 4, 4, 0, 0, 0, 0, time.Local)
	dv, err := Date(d).Value()
	if assert.NoError(t, err) {
		assert.Equal(t, []byte("'2016-04-04'"), dv)
	}
}

func TestUInt64(t *testing.T) {
	u := uint64(1) << 63
	dv, err := UInt64(u).Value()
	if assert.NoError(t, err) {
		assert.Equal(t, []byte("9223372036854775808"), dv)
	}
}

func TestDecimal(t *testing.T) {
	dv, err := Decimal32("1000", 4).Value()
	if assert.NoError(t, err) {
		assert.Equal(t, []byte("toDecimal32(1000, 4)"), dv)
	}

	dv, err = Decimal64(100, 1).Value()
	if assert.NoError(t, err) {
		assert.Equal(t, []byte("toDecimal64(100, 1)"), dv)
	}
	dv, err = Decimal128(100.01, 1).Value()
	if assert.NoError(t, err) {
		assert.Equal(t, []byte("toDecimal128(100.01, 1)"), dv)
	}
}

func TestIP(t *testing.T) {
	ipv4 := net.ParseIP("127.0.0.1")
	assert.NotNil(t, ipv4)
	ipv6 := net.ParseIP("2001:44c8:129:2632:33:0:252:2")
	assert.NotNil(t, ipv6)
	dv, err := IP(ipv4).Value()
	assert.NoError(t, err)
	assert.Equal(t, "127.0.0.1", dv)
	dv, err = IP(ipv6).Value()
	assert.NoError(t, err)
	assert.Equal(t, "2001:44c8:129:2632:33:0:252:2", dv)
}
