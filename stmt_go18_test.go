//go:build go1.8
// +build go1.8

package clickhouse

import (
	"context"
	"database/sql/driver"
	"time"
)

var (
	_ driver.StmtExecContext  = new(stmt)
	_ driver.StmtQueryContext = new(stmt)
)

func (s *stmtSuite) TestQueryContext() {
	ctx, cancel := context.WithCancel(context.Background())
	st, err := s.conn.PrepareContext(ctx, "SELECT sleep(?)")
	s.Require().NoError(err)
	time.AfterFunc(5*time.Millisecond, cancel)
	_, err = st.QueryContext(ctx, 3)
	s.EqualError(err, "doRequest: transport failed to send a request to ClickHouse: context canceled")
	s.NoError(st.Close())
}

func (s *stmtSuite) TestExecContext() {
	ctx, cancel := context.WithCancel(context.Background())
	st, err := s.conn.PrepareContext(ctx, "SELECT sleep(?)")
	s.Require().NoError(err)
	time.AfterFunc(5*time.Millisecond, cancel)
	_, err = st.ExecContext(ctx, 3)
	s.EqualError(err, "doRequest: transport failed to send a request to ClickHouse: context canceled")
	s.NoError(st.Close())
}

func (s *stmtSuite) TestExecMultiContext() {
	ctx, cancel := context.WithCancel(context.Background())
	tx, err := s.conn.BeginTx(ctx, nil)
	s.Require().NoError(err)
	st, err := tx.PrepareContext(ctx, "SELECT sleep(?)")
	s.Require().NoError(err)
	time.AfterFunc(10*time.Millisecond, cancel)
	_, err = st.ExecContext(ctx, 3)
	s.EqualError(err, "doRequest: transport failed to send a request to ClickHouse: context canceled")
	s.NoError(st.Close())
}
