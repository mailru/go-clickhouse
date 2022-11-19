package clickhouse

import (
	"compress/gzip"
	"context"
	"database/sql"
	"database/sql/driver"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
)

var (
	_ driver.Conn    = new(conn)
	_ driver.Execer  = new(conn) // nolint:staticcheck
	_ driver.Queryer = new(conn) // nolint:staticcheck
	_ driver.Tx      = new(conn)
)

type connSuite struct {
	chSuite
}

func (s *connSuite) TestQuery() {
	testCases := []struct {
		query    string
		args     []interface{}
		expected [][]interface{}
	}{
		{"SELECT i64 AS num FROM data WHERE i64<0", nil, [][]interface{}{{int64(-1)}, {int64(-2)}, {int64(-3)}}},
		{"SELECT i64 AS num FROM data WHERE i64<?", []interface{}{-3}, [][]interface{}{}},
		{"SELECT i64 AS num FROM data WHERE i64=?", []interface{}{nil}, [][]interface{}{}},
		{"SELECT i64 AS num FROM data WHERE i64=?", []interface{}{-1}, [][]interface{}{{int64(-1)}}},
		{"SELECT d32 AS num FROM data WHERE d32=?", []interface{}{Decimal32(10.1111, 4)}, [][]interface{}{{"10.1111"}}},
		{"SELECT d64 AS num FROM data WHERE d64=?", []interface{}{Decimal64(100.1111, 4)}, [][]interface{}{{"100.1111"}}},
		{"SELECT d128 AS num FROM data WHERE d128=?", []interface{}{Decimal128(1000.1111, 4)}, [][]interface{}{{"1000.1111"}}},
		{
			"SELECT * FROM data WHERE u64=?",
			[]interface{}{1},
			[][]interface{}{
				{
					int64(-1), uint64(1), float64(1), true, "1", "1", []int16{1}, []uint8{10},
					parseDate("2011-03-06"), parseDateTime("2011-03-06 06:20:00"), "one",
					"10.1111", "100.1111", "1000.1111", "1.1111", "127.0.0.1",
					"2001:db8:3333:4444:5555:6666:7777:8888", "12345678", "one",
					map[string][]int64{"key1": {1}},
				},
			},
		},
		{
			"SELECT i64, count() FROM data WHERE i64<0 GROUP BY i64 WITH TOTALS ORDER BY i64",
			nil,
			[][]interface{}{{int64(-3), int64(1)}, {int64(-2), int64(1)}, {int64(-1), int64(1)}, {int64(0), int64(3)}},
		},
	}

	doTests := func(conn *sql.DB) {
		for _, tc := range testCases {
			rows, err := conn.Query(tc.query, tc.args...)
			if !s.NoError(err) {
				continue
			}
			if len(tc.expected) == 0 {
				s.False(rows.Next())
				s.NoError(rows.Err())
			} else {
				v, err := scanValues(rows, tc.expected[0])
				if s.NoError(err) {
					s.Equal(tc.expected, v)
				}
			}
			s.NoError(rows.Close())
		}
	}

	// Tests on regular connection
	doTests(s.conn)

	// Tests on connections with enabled compression
	doTests(s.connWithCompression)

	// Tests on connection with enabled kill connection
	doTests(s.connWithKillQuery)
}

func (s *connSuite) TestExec() {
	testCases := []struct {
		query  string
		query2 string
		args   []interface{}
	}{
		{
			"INSERT INTO data (i64) VALUES (?)",
			"SELECT i64 FROM data WHERE i64=?",
			[]interface{}{int64(1)},
		},
		{
			"INSERT INTO data (i64, u64) VALUES (?, ?)",
			"SELECT i64, u64 FROM data WHERE i64=? AND u64=?",
			[]interface{}{int64(2), uint64(12)},
		},
		{
			"INSERT INTO data (i64, a16, a8) VALUES (?, ?, ?)",
			"",
			[]interface{}{int64(3), Array([]int16{1, 2}), Array([]uint8{10, 20})},
		},
		{
			"INSERT INTO data (u64) VALUES (?)",
			"SELECT u64 FROM data WHERE u64=?",
			[]interface{}{UInt64(maxAllowedUInt64)},
		},
		{
			"INSERT INTO data (u64) VALUES (?)",
			"SELECT u64 FROM data WHERE u64=?",
			[]interface{}{UInt64(maxAllowedUInt64*2 + 1)},
		},
		{
			"INSERT INTO data (d32, d64, d128) VALUES(?, ?, ?)",
			"",
			[]interface{}{Decimal32(50, 4), Decimal64(500, 4), Decimal128(5000, 4)},
		},

		{
			"INSERT INTO data (d, t) VALUES (?, ?)",
			"",
			[]interface{}{
				Date(time.Date(2016, 4, 4, 0, 0, 0, 0, time.Local)),
				time.Date(2016, 4, 4, 0, 0, 0, 0, time.Local),
			},
		},
	}
	for _, tc := range testCases {
		result, err := s.conn.Exec(tc.query, tc.args...)
		if !s.NoError(err) {
			continue
		}
		s.NotNil(result)
		_, err = result.LastInsertId()
		s.Equal(ErrNoLastInsertID, err)
		_, err = result.RowsAffected()
		s.Equal(ErrNoRowsAffected, err)
		if len(tc.query2) == 0 {
			continue
		}
		rows, err := s.conn.Query(tc.query2, tc.args...)
		if !s.NoError(err) {
			continue
		}
		v, err := scanValues(rows, tc.args)
		if s.NoError(err) {
			s.Equal([][]interface{}{tc.args}, v)
		}
		s.NoError(rows.Close())
	}
}

func (s *connSuite) TestCommit() {
	tx, err := s.conn.Begin()
	s.Require().NoError(err)
	_, err = tx.Exec("INSERT INTO data (i64) VALUES (5)")
	s.Require().NoError(err)
	s.NoError(tx.Commit())
}

func (s *connSuite) TestRollback() {
	tx, err := s.conn.Begin()
	s.Require().NoError(err)
	_, err = tx.Exec("INSERT INTO data (i64) VALUES (6)")
	s.Require().NoError(err)
	s.Equal(sql.ErrTxDone, tx.Rollback())
}

func (s *connSuite) TestServerError() {
	_, err := s.conn.Query("SELECT 1 FROM '???'")
	srvErr, ok := err.(*Error)
	s.Require().True(ok, err.Error())
	s.Equal(62, srvErr.Code)
	s.Contains(srvErr.Message, "Syntax error:")
	s.Contains(srvErr.Error(), "Code: 62, Message: Syntax error:")
}

func (s *connSuite) TestServerKillQuery() {
	// kill query and check if it is cancelled
	queryID := uuid.New().String()
	_, err := s.connWithKillQuery.QueryContext(context.WithValue(context.Background(), QueryID, queryID), "SELECT sleep(3)")
	s.Error(err)
	s.Contains(err.Error(), "net/http: timeout awaiting response headers")
	rows := s.connWithKillQuery.QueryRow("SELECT count(query_id) FROM system.processes where query_id=? and is_cancelled=?", queryID, 1)
	var amount int
	err = rows.Scan(&amount)
	s.NoError(err)
	s.Equal(1, amount)

	// not kill query and check if it is not cancelled
	queryID = uuid.New().String()
	_, err = s.connWithKillQuery.QueryContext(context.WithValue(context.Background(), QueryID, queryID), "SELECT sleep(0.1)")
	s.NoError(err)
	rows = s.connWithKillQuery.QueryRow("SELECT count(query_id) FROM system.processes where query_id=? and is_cancelled=?", queryID, 1)
	err = rows.Scan(&amount)
	s.NoError(err)
	s.Equal(0, amount)

	_, err = s.conn.QueryContext(context.Background(), "SELECT sleep(2)")
	s.NoError(err)
}

func (s *connSuite) TestBuildRequestReadonlyWithAuth() {
	cfg := NewConfig()
	cfg.User = "user"
	cfg.Password = "password"
	cn := newConn(cfg)
	req, err := cn.buildRequest(context.Background(), "SELECT 1", nil)
	if s.NoError(err) {
		user, password, ok := req.BasicAuth()
		s.True(ok)
		s.Equal("user", user)
		s.Equal("password", password)
		s.Equal(http.MethodPost, req.Method)
		s.Equal(cn.url.String(), req.URL.String())
		s.Nil(req.URL.User)
		b, err := ioutil.ReadAll(req.Body)
		s.Require().NoError(err)
		s.Equal("SELECT 1", string(b))
	}
}

func (s *connSuite) TestBuildRequestReadWriteWOAuth() {
	cn := newConn(NewConfig())
	req, err := cn.buildRequest(context.Background(), "INSERT 1 INTO num", nil)
	if s.NoError(err) {
		_, _, ok := req.BasicAuth()
		s.False(ok)
		s.Equal(http.MethodPost, req.Method)
		s.Equal(cn.url.String(), req.URL.String())
	}
}

func (s *connSuite) TestBuildRequestWithQueryId() {
	cn := newConn(NewConfig())
	testCases := []struct {
		queryID  string
		expected string
	}{
		{
			"",
			cn.url.String(),
		},
		{
			"query-id",
			cn.url.String() + "&query_id=query-id",
		},
		{
			"query id",
			cn.url.String() + "&query_id=query+id",
		},
		{
			" ",
			cn.url.String() + "&query_id=+",
		},
		{
			"_",
			cn.url.String() + "&query_id=_",
		},
		{
			"^",
			cn.url.String() + "&query_id=%5E",
		},
		{
			"213&query=select 1",
			cn.url.String() + "&query_id=213%26query%3Dselect+1",
		},
	}
	for _, tc := range testCases {
		req, err := cn.buildRequest(context.WithValue(context.Background(), QueryID, tc.queryID), "INSERT 1 INTO num", nil)
		if s.NoError(err) {
			s.Equal(http.MethodPost, req.Method)
			s.Equal(tc.expected, req.URL.String())
		}
	}
}
func (s *connSuite) TestBuildRequestWithQuotaKey() {
	cn := newConn(NewConfig())
	testCases := []struct {
		quotaKey string
		expected string
	}{
		{
			"",
			cn.url.String(),
		},
		{
			"quota-key",
			cn.url.String() + "&quota_key=quota-key",
		},
		{
			"quota key",
			cn.url.String() + "&quota_key=quota+key",
		},
		{
			" ",
			cn.url.String() + "&quota_key=+",
		},
		{
			"_",
			cn.url.String() + "&quota_key=_",
		},
		{
			"^",
			cn.url.String() + "&quota_key=%5E",
		},
		{
			"213&query=select 1",
			cn.url.String() + "&quota_key=213%26query%3Dselect+1",
		},
	}
	for _, tc := range testCases {
		req, err := cn.buildRequest(context.WithValue(context.Background(), QuotaKey, tc.quotaKey), "SELECT 1", nil)
		if s.NoError(err) {
			s.Equal(http.MethodPost, req.Method)
			s.Equal(tc.expected, req.URL.String())
		}
	}
}
func (s *connSuite) TestBuildRequestWithQueryIdAndQuotaKey() {
	cn := newConn(NewConfig())
	testCases := []struct {
		quotaKey string
		queryID  string
		expected string
	}{
		{
			"",
			"",
			cn.url.String(),
		},
		{
			"quota-key",
			"query-id",
			cn.url.String() + "&query_id=query-id&quota_key=quota-key",
		},
		{
			"quota key",
			"query id",
			cn.url.String() + "&query_id=query+id&quota_key=quota+key",
		},
		{
			" ",
			" ",
			cn.url.String() + "&query_id=+&quota_key=+",
		},
		{
			"_",
			"_",
			cn.url.String() + "&query_id=_&quota_key=_",
		},
		{
			"^",
			"&query",
			cn.url.String() + "&query_id=%26query&quota_key=%5E",
		},
	}
	for _, tc := range testCases {
		ctx := context.Background()
		ctx = context.WithValue(ctx, QuotaKey, tc.quotaKey)
		ctx = context.WithValue(ctx, QueryID, tc.queryID)
		req, err := cn.buildRequest(ctx, "SELECT 1", nil)
		if s.NoError(err) {
			s.Equal(http.MethodPost, req.Method)
			s.Equal(tc.expected, req.URL.String())
		}
	}
}
func (s *connSuite) TestBuildRequestParamsInterpolation() {
	query := `INSERT INTO test (str) VALUES ("Question?")`
	cn := newConn(NewConfig())
	req, err := cn.buildRequest(context.Background(), query, make([]driver.Value, 0))
	if s.NoError(err) {
		body, e := ioutil.ReadAll(req.Body)
		if s.NoError(e) {
			s.Equal(query, string(body))
		}
	}
}

func (s *connSuite) TestRequestBodyGzipCompression() {
	query := `INSERT INTO test (str) VALUES ("Question?")`
	cn := newConn(NewConfig())
	cn.useGzipCompression = true
	req, err := cn.buildRequest(context.Background(), query, make([]driver.Value, 0))
	if s.NoError(err) {
		s.Contains(req.Header, "Content-Encoding")
		gz, err := gzip.NewReader(req.Body)
		if s.NoError(err) {
			defer gz.Close()
			body, e := ioutil.ReadAll(gz)
			if s.NoError(e) {
				s.Equal(query, string(body))
			}
		}
	}
}

func (s *connSuite) TestLongRequest() {
	expected := strings.Repeat("x", 100000)
	rows, err := s.conn.Query("SELECT ?", expected)
	if s.NoError(err) {
		rows.Next()
		var actual string
		err = rows.Scan(&actual)
		if s.NoError(err) {
			s.Equal(expected, actual)
		}
	}
}

func TestConn(t *testing.T) {
	suite.Run(t, new(connSuite))
}
