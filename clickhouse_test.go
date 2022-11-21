package clickhouse

import (
	"database/sql"
	"database/sql/driver"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/stretchr/testify/suite"
)

var (
	_ driver.Driver = new(chDriver)
)

var ddls = []string{
	`DROP TABLE IF EXISTS data`,
	`CREATE TABLE data (
			i64 Int64,
			u64 UInt64,
			f64 Float64,
			b   Boolean,
			s   String,
			s2  String,
			a16 Array(Int16),
			a8  Array(UInt8),
			d   Date,
			t   DateTime,
			e   Enum8('one'=1, 'two'=2, 'three'=3),
			d32 Decimal32(4),
			d64 Decimal64(4),
			d128 Decimal128(4),
			d10 Decimal(10, 4),
			ipv4 IPv4,
			ipv6 IPv6,
			fs   FixedString(8),
			lc   LowCardinality(String),
			m    Map(String, Array(Int64))
	) ENGINE = Memory`,
	`INSERT INTO data VALUES
		(-1, 1, 1.0, true, '1', '1', [1], [10], '2011-03-06', '2011-03-06 06:20:00', 'one',   '10.1111', '100.1111', '1000.1111', '1.1111', '127.0.0.1',       '2001:db8:3333:4444:5555:6666:7777:8888', '12345678', 'one',   {'key1':[1]}),
		(-2, 2, 2.0, false, '2', '2', [2], [20], '2012-05-31', '2012-05-31 11:20:00', 'two',   '30.1111', '300.1111', '2000.1111', '2.1111', '8.8.8.8',         '2001:db8:3333:4444:CCCC:DDDD:EEEE:FFFF', '88888888', 'two',   {'key2':[2]}),
		(-3, 3, 3.0, true, '3', '2', [3], [30], '2016-04-04', '2016-04-04 11:30:00', 'three', '40.1111', '400.1111', '3000.1111', '3.1111', '255.255.255.255', '::1234:5678',                            '87654321', 'three', {'key3':[3]})
	`,
}

var initialzer = new(dbInit)

type dbInit struct {
	mu   sync.Mutex
	done bool
}

type chSuite struct {
	suite.Suite
	conn                *sql.DB
	connWithCompression *sql.DB
	connWithKillQuery   *sql.DB
}

func (s *chSuite) SetupSuite() {
	dsn := os.Getenv("TEST_CLICKHOUSE_DSN")
	if len(dsn) == 0 {
		dsn = "http://localhost:8123/default"
	}

	conn, err := sql.Open("chhttp", dsn)
	s.Require().NoError(err)
	s.Require().NoError(initialzer.Do(conn))
	s.conn = conn

	connWithCompression, err := sql.Open("chhttp", dsn+"?enable_http_compression=1")
	s.Require().NoError(err)
	s.connWithCompression = connWithCompression

	connWithKillQuery, err := sql.Open("chhttp", dsn+"?kill_query=1&read_timeout=1s")
	s.Require().NoError(err)
	s.connWithKillQuery = connWithKillQuery
}

func (s *chSuite) TearDownSuite() {
	s.conn.Close()
	_, err := s.conn.Query("SELECT 1")
	s.EqualError(err, "sql: database is closed")

	s.connWithCompression.Close()
	_, err = s.connWithCompression.Query("SELECT 1")
	s.EqualError(err, "sql: database is closed")

	s.connWithKillQuery.Close()
	_, err = s.connWithKillQuery.Query("SELECT 1")
	s.EqualError(err, "sql: database is closed")
}

func (d *dbInit) Do(conn *sql.DB) error {
	if d.done {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.done {
		return nil
	}
	for _, ddl := range ddls {
		if _, err := conn.Exec(ddl); err != nil {
			return err
		}
	}
	d.done = true
	return nil
}

func scanValues(rows *sql.Rows, template []interface{}) (interface{}, error) {
	var result [][]interface{}
	types := make([]reflect.Type, len(template))
	for i, v := range template {
		types[i] = reflect.TypeOf(v)
	}
	ptrs := make([]interface{}, len(types))
	var err error
	for rows.Next() {
		if err = rows.Err(); err != nil {
			return nil, err
		}
		for i, t := range types {
			ptrs[i] = reflect.New(t).Interface()
		}
		err = rows.Scan(ptrs...)
		if err != nil {
			return nil, err
		}
		values := make([]interface{}, len(types))
		for i, p := range ptrs {
			values[i] = reflect.ValueOf(p).Elem().Interface()
		}
		result = append(result, values)
	}
	return result, nil
}

func parseTime(layout, s string) time.Time {
	t, err := time.Parse(layout, s)
	if err != nil {
		panic(err)
	}
	return t
}

func parseDate(s string) time.Time {
	return parseTime(dateFormat, s)
}

func parseDateTime(s string) time.Time {
	return parseTime(timeFormat, s)
}
