// +build go1.8

package clickhouse

import (
	"context"
	"database/sql/driver"
	"time"
)

var (
	_ driver.ExecerContext      = new(conn)
	_ driver.QueryerContext     = new(conn)
	_ driver.ConnPrepareContext = new(conn)
	_ driver.ConnBeginTx        = new(conn)
	_ driver.Pinger             = new(conn)
)

func (s *connSuite) TestQueryContext() {
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(5*time.Millisecond, cancel)
	_, err := s.conn.QueryContext(ctx, "SELECT sleep(3)")
	s.EqualError(err, "context canceled")
}

func (s *connSuite) TestExecContext() {
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(5*time.Millisecond, cancel)
	_, err := s.conn.ExecContext(ctx, "SELECT sleep(3)")
	s.EqualError(err, "context canceled")
}

func (s *connSuite) TestPing() {
	s.NoError(s.conn.Ping())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.EqualError(s.conn.PingContext(ctx), "context canceled")
}

func (s *connSuite) TestColumnTypes() {
	rows, err := s.conn.Query("SELECT * FROM data LIMIT 1")
	s.Require().NoError(err)
	defer rows.Close()
	types, err := rows.ColumnTypes()
	s.Require().NoError(err)
	expected := []string{
		"Int64", "UInt64", "Float64", "String", "String", "Array(Int16)", "Array(UInt8)", "Date", "DateTime",
		"Enum8('one' = 1, 'two' = 2, 'three' = 3)",
		"Decimal(9, 4)", "Decimal(18, 4)", "Decimal(38, 4)", "Decimal(10, 4)",
	}
	s.Require().Equal(len(expected), len(types))
	for i, e := range expected {
		s.Equal(e, types[i].DatabaseTypeName())
	}
}
