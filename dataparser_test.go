package clickhouse

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseData(t *testing.T) {
	type testCase struct {
		name          string
		inputtype     string
		inputopt      *DataParserOptions
		inputdata     string
		output        interface{}
		failParseDesc bool
		failNewParser bool
		failParseData bool
	}

	losAngeles, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Fatalf("failed to load time zone America/Los_Angeles: %v", err)
	}
	moscow, err := time.LoadLocation("Europe/Moscow")
	if err != nil {
		t.Fatalf("failed to load time zone Europe/Moscow: %v", err)
	}

	testCases := []*testCase{
		{
			name:      "nullable string",
			inputtype: "Nullable(String)",
			inputdata: `\N`,
			output:    nil,
		},
		{
			name:      "nullable int not null",
			inputtype: "Nullable(UInt64)",
			inputdata: "655",
			output:    uint64(655),
		},
		{
			name:      "string",
			inputtype: "String",
			inputdata: "hello world",
			output:    "hello world",
		},
		{
			name:      "fixed string",
			inputtype: "FixedString(10)",
			inputdata: `hello\0\0\0\0\0`,
			output:    "hello\x00\x00\x00\x00\x00",
		},
		{
			name:      "string with escaping",
			inputtype: "String",
			inputdata: `hello \'world`,
			output:    "hello 'world",
		},
		{
			name:          "string with incorrect escaping",
			inputtype:     "String",
			inputdata:     `hello world\`,
			failParseData: true,
		},
		{
			name:      "int",
			inputtype: "UInt64",
			inputdata: "123",
			output:    uint64(123),
		},
		{
			name:      "float",
			inputtype: "Float32",
			inputdata: "-inf",
			output:    float32(math.Inf(-1)),
		},
		{
			name:      "decimal",
			inputtype: "Decimal(9,4)",
			inputdata: "123",
			output:    "123",
		},
		{
			name:      "date",
			inputtype: "Date",
			inputdata: "2018-01-02",
			output:    time.Date(2018, 1, 2, 0, 0, 0, 0, time.UTC),
		},
		{
			name:      "zero date",
			inputtype: "Date",
			inputdata: "0000-00-00",
			output:    time.Time{},
		},
		{
			name:      "with special timezone",
			inputtype: "Date",
			inputdata: "2019-06-29",
			inputopt: &DataParserOptions{
				Location: losAngeles,
			},
			output: time.Date(2019, 6, 29, 0, 0, 0, 0, losAngeles),
		},
		{
			name:      "enum",
			inputtype: "Enum8('hello' = 1, 'world' = 2)",
			inputdata: "hello",
			output:    "hello",
		},
		{
			name:      "uuid",
			inputtype: "UUID",
			inputdata: "c79a9747-7cef-4b11-8177-380f7ed462a4",
			output:    "c79a9747-7cef-4b11-8177-380f7ed462a4",
		},
		{
			name:      "datetime, without options and argument",
			inputtype: "DateTime",
			inputdata: "2018-01-02 12:34:56",
			output:    time.Date(2018, 1, 2, 12, 34, 56, 0, time.UTC),
		},
		{
			name:      "datetime, with argument",
			inputtype: "DateTime('America/Los_Angeles')",
			inputdata: "2018-01-02 12:34:56",
			output:    time.Date(2018, 1, 2, 12, 34, 56, 0, losAngeles),
		},
		{
			name:      "datetime with argument, but location nil",
			inputtype: "DateTime('America/Los_Angeles')",
			inputdata: "2018-01-02 12:34:56",
			inputopt: &DataParserOptions{
				Location: nil,
			},
			output: time.Date(2018, 1, 2, 12, 34, 56, 0, losAngeles),
		},
		{
			name:      "datetime without argument, but use location",
			inputtype: "DateTime",
			inputdata: "2018-01-02 12:34:56",
			inputopt: &DataParserOptions{
				Location: moscow,
			},
			output: time.Date(2018, 1, 2, 12, 34, 56, 0, moscow),
		},
		{
			name:      "datetime with argument and location, ingnore argument",
			inputtype: "DateTime('America/Los_Angeles')",
			inputdata: "2018-01-02 12:34:56",
			inputopt: &DataParserOptions{
				Location: moscow,
			},
			output: time.Date(2018, 1, 2, 12, 34, 56, 0, moscow),
		},
		{
			name:      "datetime with argument and location, prefer argument",
			inputtype: "DateTime('America/Los_Angeles')",
			inputdata: "2018-01-02 12:34:56",
			inputopt: &DataParserOptions{
				Location:      moscow,
				UseDBLocation: true,
			},
			output: time.Date(2018, 1, 2, 12, 34, 56, 0, losAngeles),
		},
		{
			name:          "datetime in nowhere",
			inputtype:     "DateTime('Nowhere')",
			inputdata:     "2018-01-02 12:34:56",
			failNewParser: true,
		},
		{
			name:      "zero datetime",
			inputtype: "DateTime",
			inputdata: "0000-00-00 00:00:00",
			output:    time.Time{},
		},
		{
			name:          "short datetime",
			inputtype:     "DateTime",
			inputdata:     "000-00-00 00:00:00",
			output:        time.Time{},
			failParseData: true,
		},
		{
			name:          "malformed datetime",
			inputtype:     "DateTime",
			inputdata:     "a000-00-00 00:00:00",
			output:        time.Time{},
			failParseData: true,
		},
		{
			name:      "tuple",
			inputtype: "Tuple(String, Float64, Int16, UInt16, Int64)",
			inputdata: "('hello world',32.1,-1,2,3)",
			output: struct {
				Field0 string
				Field1 float64
				Field2 int16
				Field3 uint16
				Field4 int64
			}{"hello world", 32.1, -1, 2, 3},
		},
		{
			name:      "array of strings",
			inputtype: "Array(String)",
			inputdata: `['hello world\',','goodbye galaxy']`,
			output:    []string{"hello world',", "goodbye galaxy"},
		},
		{
			name:          "array of unquoted strings",
			inputtype:     "Array(String)",
			inputdata:     "[hello,world]",
			failParseData: true,
		},
		{
			name:          "array with unfinished quoted string",
			inputtype:     "Array(String)",
			inputdata:     "['hello','world]",
			failParseData: true,
		},
		{
			name:      "array of ints",
			inputtype: "Array(UInt64)",
			inputdata: "[1,2,3]",
			output:    []uint64{1, 2, 3},
		},
		{
			name:      "array of dates",
			inputtype: "Array(Date)",
			inputdata: "['2018-01-02','0000-00-00']",
			output: []time.Time{
				time.Date(2018, 1, 2, 0, 0, 0, 0, time.UTC),
				{},
			},
		},
		{
			name:      "empty array of ints",
			inputtype: "Array(Int8)",
			inputdata: "[]",
			output:    []int8{},
		},
		{
			name:      "empty array of nothing",
			inputtype: "Array(Nothing)",
			inputdata: "[]",
			output:    []struct{}{},
		},
		{
			name:      "array of tuples",
			inputtype: "Array(Tuple(String, Float32))",
			inputdata: "[('hello world',32.1),('goodbye galaxy',42.0)]",
			output: []struct {
				Field0 string
				Field1 float32
			}{
				{
					"hello world",
					float32(32.1),
				},
				{
					"goodbye galaxy",
					float32(42.0),
				},
			},
		},
		{
			name:          "malformed array element",
			inputtype:     "Array(UInt8)",
			inputdata:     "[1,2,'3']",
			failParseData: true,
		},
		{
			name:          "array without left bracket",
			inputtype:     "Array(Int8)",
			inputdata:     "1,2,3]",
			failParseData: true,
		},
		{
			name:          "array without right bracket",
			inputtype:     "Array(UInt64)",
			inputdata:     "[1,2,3",
			failParseData: true,
		},
		{
			name:          "wrong character between tuple elements",
			inputtype:     "Tuple(String, String)",
			inputdata:     "('hello'.'world')",
			failParseData: true,
		},
		{
			name:          "malformed tuple element",
			inputtype:     "Tuple(UInt32, Int32)",
			inputdata:     "(1,'2')",
			failParseData: true,
		},
		{
			name:          "tuple without left paren",
			inputtype:     "Tuple(Int8, Int8)",
			inputdata:     "1,2)",
			failParseData: true,
		},
		{
			name:          "tuple without right paren",
			inputtype:     "Tuple(UInt8, Int8)",
			inputdata:     "(1,2",
			failParseData: true,
		},
		{
			name:      "low cardinality string",
			inputtype: "LowCardinality(String)",
			inputdata: "hello",
			output:    "hello",
		},
		{
			name:      "low cardinality string with escaping",
			inputtype: "LowCardinality(String)",
			inputdata: `hello \'world`,
			output:    "hello 'world",
		},
		{
			name:          "low cardinality string with incorrect escaping",
			inputtype:     "LowCardinality(String)",
			inputdata:     `hello world\`,
			failParseData: true,
		},
		{
			name:      "low cardinality UInt64",
			inputtype: "LowCardinality(UInt64)",
			inputdata: "123",
			output:    uint64(123),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			desc, err := ParseTypeDesc(tc.inputtype)
			if tc.failParseDesc {
				assert.Error(tt, err)
				return
			}
			if !assert.NoError(tt, err) {
				return
			}

			parser, err := newDataParser(desc, false, tc.inputopt)
			if tc.failNewParser {
				assert.Error(tt, err)
				return
			}
			if !assert.NoError(tt, err) {
				return
			}

			output, err := parser.Parse(strings.NewReader(tc.inputdata))
			if tc.failParseData {
				assert.Error(tt, err)
				return
			}
			if !assert.NoError(tt, err) {
				return
			}

			assert.Equal(tt, tc.output, output)
		})
	}
}
