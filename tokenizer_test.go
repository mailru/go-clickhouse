package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTokenize(t *testing.T) {
	type testCase struct {
		name   string
		input  string
		output []*token
		fail   bool
	}
	testCases := []*testCase{
		{
			name:   "empty",
			input:  "",
			output: []*token{{eof, ""}},
		},
		{
			name:   "only whitespace",
			input:  "",
			output: []*token{{eof, ""}},
		},
		{
			name:  "whitespace all over the place",
			input: "   \t\nhello   \t  \n   world   \n",
			output: []*token{
				{'s', "hello"},
				{'s', "world"},
				{eof, ""},
			},
		},
		{
			name:  "complex with quotes and escaping",
			input: `Array(Tuple(FixedString(5), Float32, 'hello, \') world'))`,
			output: []*token{
				{'s', "Array"},
				{'(', ""},
				{'s', "Tuple"},
				{'(', ""},
				{'s', "FixedString"},
				{'(', ""},
				{'s', "5"},
				{')', ""},
				{',', ""},
				{'s', "Float32"},
				{',', ""},
				{'q', `hello, ') world`},
				{')', ""},
				{')', ""},
				{eof, ""},
			},
		},
		{
			name:  "unclosed quote",
			input: "Array(')",
			fail:  true,
		},
		{
			name:  "unfinished escape",
			input: `Array('\`,
			fail:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			output, err := tokenizeString(tc.input)
			if tc.fail {
				assert.Error(tt, err)
			} else {
				assert.NoError(tt, err)
				assert.Equal(tt, tc.output, output)
			}
		})
	}
}
