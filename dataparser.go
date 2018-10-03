package clickhouse

import (
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// DataParser implements parsing of a driver value and reporting its type.
type DataParser interface {
	Parse(io.RuneScanner) (driver.Value, error)
	Type() reflect.Type
}

type stringParser struct {
	unquote bool
	length int
}

type dateTimeParser struct {
	unquote bool
	format string
	location *time.Location
}

func readNumber(s io.RuneScanner) (string, error) {
	var builder strings.Builder

loop:
	for {
		r := read(s)

		switch r {
		case eof:
			break loop
		case ',', ']', ')':
			s.UnreadRune()
			break loop
		}

		builder.WriteRune(r)
	}

	return builder.String(), nil
}

func readUnquoted(s io.RuneScanner, length int) (string, error) {
	var builder strings.Builder

	runesRead := 0
loop:
	for length == 0 || runesRead < length {
		r := read(s)

		switch r {
		case eof:
			break loop
		case '\\':
			escaped, err := readEscaped(s)
			if err != nil {
				return "", fmt.Errorf("incorrect escaping in string: %v", err)
			}
			r = escaped
		case '\'':
			s.UnreadRune()
			break loop
		}

		builder.WriteRune(r)
		runesRead++
	}

	if length != 0 && runesRead != length {
		return "", fmt.Errorf("unexpected string length %d, expected %d", runesRead, length)
	}

	return builder.String(), nil
}

func readString(s io.RuneScanner, length int, unquote bool) (string, error) {
	if unquote {
		if r := read(s); r != '\'' {
			return "", fmt.Errorf("unexpected character instead of a quote")
		}
	}

	str, err := readUnquoted(s, length)
	if err != nil {
		return "", fmt.Errorf("failed to read string")
	}

	if unquote {
		if r := read(s); r != '\'' {
			return "", fmt.Errorf("unexpected character instead of a quote")
		}
	}

	return str, nil
}

func (p *stringParser) Parse(s io.RuneScanner) (driver.Value, error) {
	return readString(s, p.length, p.unquote)
}

func (p *stringParser) Type() reflect.Type {
	return reflect.ValueOf("").Type()
}

func (p *dateTimeParser) Parse(s io.RuneScanner) (driver.Value, error) {
	str, err := readString(s, len(p.format), p.unquote)
	if err != nil {
		return nil, fmt.Errorf("failed to read the string representation of date or datetime: %v", err)
	}

	if str == "0000-00-00" || str == "0000-00-00 00:00:00" {
		return time.Time{}, nil
	}

	return time.ParseInLocation(p.format, str, p.location)
}

func (p *dateTimeParser) Type() reflect.Type {
	return reflect.ValueOf(time.Time{}).Type()
}

type arrayParser struct{
	arg DataParser
}

func (p *arrayParser) Type() reflect.Type {
	return reflect.SliceOf(p.arg.Type())
}

type tupleParser struct{
	args []DataParser
}

func (p *tupleParser) Type() reflect.Type {
	fields := make([]reflect.StructField, len(p.args), len(p.args))
	for i, arg := range p.args {
		fields[i].Name = "Field" + strconv.Itoa(i)
		fields[i].Type = arg.Type()
	}
	return reflect.StructOf(fields)
}

func (p *tupleParser) Parse(s io.RuneScanner) (driver.Value, error) {
	r := read(s)
	if r != '(' {
		return nil, fmt.Errorf("unexpected character '%c', expected '(' at the beginning of tuple", r)
	}

	struc := reflect.New(p.Type()).Elem()
	for i, arg := range p.args {
		if i > 0 {
			r := read(s)
			if r != ',' {
				return nil, fmt.Errorf("unexpected character '%c', expected ',' between tuple elements", r)
			}
		}

		v, err := arg.Parse(s)
		if err != nil {
			return nil, fmt.Errorf("failed to parse tuple element: %v", err)
		}

		struc.Field(i).Set(reflect.ValueOf(v))
	}

	r = read(s)
	if r != ')' {
		return nil, fmt.Errorf("unexpected character '%c', expected ')' at the end of tuple", r)
	}

	return struc.Interface(), nil
}


func (p *arrayParser) Parse(s io.RuneScanner) (driver.Value, error) {
	r := read(s)
	if r != '[' {
		return nil, fmt.Errorf("unexpected character '%c', expected '[' at the beginning of array", r)
	}

	slice := reflect.MakeSlice(p.Type(), 0, 0)
	for i := 0;; i++ {
		r := read(s)
		s.UnreadRune()
		if r == ']' {
			break
		}

		v, err := p.arg.Parse(s)
		if err != nil {
			return nil, fmt.Errorf("failed to parse array element: %v", err)
		}

		slice = reflect.Append(slice, reflect.ValueOf(v))

		r = read(s)
		if r != ',' {
			s.UnreadRune()
		}
	}

	r = read(s)
	if r != ']' {
		return nil, fmt.Errorf("unexpected character '%c', expected ']' at the end of array", r)
	}

	return slice.Interface(), nil
}


func newDateTimeParser(format, locname string, unquote bool) (DataParser, error) {
	loc, err := time.LoadLocation(locname)
	if err != nil {
		return nil, err
	}
	return &dateTimeParser{
		unquote: unquote,
		format: format,
		location: loc,
	}, nil
}

type intParser struct {
	signed bool
	bitSize int
}

type floatParser struct {
	bitSize int
}

func (p *intParser) Parse(s io.RuneScanner) (driver.Value, error) {
	repr, err := readNumber(s)
	if err != nil {
		return nil, err
	}

	if p.signed {
		v, err := strconv.ParseInt(repr, 10, p.bitSize)
		switch p.bitSize {
		case 8: return int8(v), err
		case 16: return int16(v), err
		case 32: return int32(v), err
		case 64: return int64(v), err
		default: panic("unsupported bit size")
		}
	} else {
		v, err := strconv.ParseUint(repr, 10, p.bitSize)
		switch p.bitSize {
		case 8: return uint8(v), err
		case 16: return uint16(v), err
		case 32: return uint32(v), err
		case 64: return uint64(v), err
		default: panic("unsupported bit size")
		}
	}
}

func (p *intParser) Type() reflect.Type {
	if p.signed {
		switch p.bitSize {
		case 8: return reflect.ValueOf(int8(0)).Type()
		case 16: return reflect.ValueOf(int16(0)).Type()
		case 32: return reflect.ValueOf(int32(0)).Type()
		case 64: return reflect.ValueOf(int64(0)).Type()
		default: panic("unsupported bit size")
		}
	} else {
		switch p.bitSize {
		case 8: return reflect.ValueOf(uint8(0)).Type()
		case 16: return reflect.ValueOf(uint16(0)).Type()
		case 32: return reflect.ValueOf(uint32(0)).Type()
		case 64: return reflect.ValueOf(uint64(0)).Type()
		default: panic("unsupported bit size")
		}
	}
}

func (p *floatParser) Parse(s io.RuneScanner) (driver.Value, error) {
	repr, err := readNumber(s)
	if err != nil {
		return nil, err
	}

	v, err := strconv.ParseFloat(repr, p.bitSize)
	switch p.bitSize {
	case 32: return float32(v), err
	case 64: return float64(v), err
	default: panic("unsupported bit size")
	}
}

func (p *floatParser) Type() reflect.Type {
	switch p.bitSize {
	case 32: return reflect.ValueOf(float32(0)).Type()
	case 64: return reflect.ValueOf(float64(0)).Type()
	default: panic("unsupported bit size")
	}
}

type nothingParser struct{}

func (p *nothingParser) Parse(s io.RuneScanner) (driver.Value, error) {
	return nil, nil
}

func (p *nothingParser) Type() reflect.Type {
	return reflect.ValueOf(struct{}{}).Type()
}

// NewDataParser creates a new DataParser based on the
// given TypeDesc.
func NewDataParser(t *TypeDesc) (DataParser, error) {
	return newDataParser(t, false)
}

func newDataParser(t *TypeDesc, unquote bool) (DataParser, error) {
	switch t.Name {
	case "Nothing": return &nothingParser{}, nil
	case "Nullable": return nil, fmt.Errorf("Nullable types are not supported")
	case "Date":
		// FIXME: support custom default/override location
		return newDateTimeParser("2006-01-02", "UTC", unquote)
	case "DateTime":
		// FIXME: support custom default/override location
		locname := "UTC"
		if len(t.Args) > 0 {
			locname = t.Args[0].Name
		}
		return newDateTimeParser("2006-01-02 15:04:05", locname, unquote)
	case "UInt8": return &intParser{false, 8}, nil
	case "UInt16": return &intParser{false, 16}, nil
	case "UInt32": return &intParser{false, 32}, nil
	case "UInt64": return &intParser{false, 64}, nil
	case "Int8": return &intParser{true, 8}, nil
	case "Int16": return &intParser{true, 16}, nil
	case "Int32": return &intParser{true, 32}, nil
	case "Int64": return &intParser{true, 64}, nil
	case "Float32": return &floatParser{32}, nil
	case "Float64": return &floatParser{64}, nil
	case "String", "Enum8", "Enum16": return &stringParser{unquote: unquote}, nil
	case "FixedString":
		if len(t.Args) != 1 {
			return nil, fmt.Errorf("length not specified for FixedString")
		}
		length, err := strconv.Atoi(t.Args[0].Name)
		if err != nil{
			return nil, fmt.Errorf("malformed length specified for FixedString: %v", err)
		}
		return &stringParser{unquote: unquote, length: length}, nil
	case "Array":
		if len(t.Args) != 1 {
			return nil, fmt.Errorf("element type not specified for Array")
		}
		subParser, err := newDataParser(t.Args[0], true)
		if err != nil {
			return nil, fmt.Errorf("failed to create parser for array elements: %v", err)
		}
		return &arrayParser{subParser}, nil
	case "Tuple":
		if len(t.Args) < 1 {
			return nil, fmt.Errorf("element types not specified for Tuple")
		}
		subParsers := make([]DataParser, len(t.Args), len(t.Args))
		for i, arg := range t.Args {
			subParser, err := newDataParser(arg, true)
			if err != nil {
				return nil, fmt.Errorf("failed to create parser for tuple element: %v", err)
			}
			subParsers[i] = subParser
		}
		return &tupleParser{subParsers}, nil
	default:
		return nil, fmt.Errorf("type %s is not supported", t.Name)
	}
}
