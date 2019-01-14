package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseTypeDesc(t *testing.T) {
	type testCase struct {
		name   string
		input  string
		output *TypeDesc
		fail   bool
	}
	testCases := []*testCase{
		{
			name:   "plain type",
			input:  "String",
			output: &TypeDesc{Name: "String"},
		},
		{
			name:  "nullable type",
			input: "Nullable(Nothing)",
			output: &TypeDesc{
				Name: "Nullable",
				Args: []*TypeDesc{{Name: "Nothing"}},
			},
		},
		{
			name:   "empty arg",
			input:  "DateTime()",
			output: &TypeDesc{Name: "DateTime"},
		},
		{
			name:  "numeric arg",
			input: "FixedString(42)",
			output: &TypeDesc{
				Name: "FixedString",
				Args: []*TypeDesc{{Name: "42"}},
			},
		},
		{
			name:   "args are ignored for Enum",
			input:  "Enum8(you can = put, 'whatever' here)",
			output: &TypeDesc{Name: "Enum8"},
		},
		{
			name:  "quoted arg",
			input: "DateTime('UTC')",
			output: &TypeDesc{
				Name: "DateTime",
				Args: []*TypeDesc{{Name: "UTC"}},
			},
		},
		{
			name:  "decimal",
			input: "Decimal(9,4)",
			output: &TypeDesc{
				Name: "Decimal",
				Args: []*TypeDesc{{Name: "9"}, {Name: "4"}},
			},
		},
		{
			name:  "quoted escaped arg",
			input: `DateTime('UTC\b\r\n\'\f\t\0')`,
			output: &TypeDesc{
				Name: "DateTime",
				Args: []*TypeDesc{{Name: "UTC\b\r\n'\f\t\x00"}},
			},
		},
		{
			name:  "nested args",
			input: "Array(Tuple(Tuple(String, String), Tuple(String, UInt64)))",
			output: &TypeDesc{
				Name: "Array",
				Args: []*TypeDesc{
					{
						Name: "Tuple",
						Args: []*TypeDesc{
							{
								Name: "Tuple",
								Args: []*TypeDesc{{Name: "String"}, {Name: "String"}},
							},
							{
								Name: "Tuple",
								Args: []*TypeDesc{{Name: "String"}, {Name: "UInt64"}},
							},
						},
					},
				},
			},
		},
		{
			name:  "unfinished arg list",
			input: "Array(Tuple(Tuple(String, String), Tuple(String, UInt64))",
			fail:  true,
		},
		{
			name:  "left paren without name",
			input: "(",
			fail:  true,
		},
		{
			name:  "unfinished quote",
			input: "Array(')",
			fail:  true,
		},
		{
			name:  "unfinished escape",
			input: `Array(\`,
			fail:  true,
		},
		{
			name:  "stuff after end",
			input: `Array() String`,
			fail:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			output, err := ParseTypeDesc(tc.input)
			if tc.fail {
				assert.Error(tt, err)
			} else {
				assert.NoError(tt, err)
			}
			assert.Equal(tt, tc.output, output)
		})
	}
}
