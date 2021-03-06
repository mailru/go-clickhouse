package clickhouse

import (
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/suite"
)

var (
	_ driver.Stmt = new(stmt)
)

type stmtSuite struct {
	chSuite
}

func (s *stmtSuite) TestQuery() {
	testCases := []struct {
		query    string
		args     [][]interface{}
		expected [][]interface{}
	}{
		{
			"SELECT i64 AS num FROM data WHERE i64=-1",
			[][]interface{}{nil, nil},
			[][]interface{}{{int64(-1)}, {int64(-1)}},
		},
		{
			"SELECT i64 AS num FROM data WHERE i64=?",
			[][]interface{}{{-2}, {-3}},
			[][]interface{}{{int64(-2)}, {int64(-3)}},
		},
		{
			"SELECT i64 AS num FROM data WHERE i64<?",
			[][]interface{}{{-3}},
			[][]interface{}{{}},
		},
	}

	for _, tc := range testCases {
		st, err := s.conn.Prepare(tc.query)
		if !s.NoError(err) {
			continue
		}
		for i, args := range tc.args {
			expected := tc.expected[i]
			rows, err := st.Query(args...)
			if !s.NoError(err) {
				continue
			}
			if len(expected) == 0 {
				s.False(rows.Next())
				s.NoError(rows.Err())
			} else {
				v, err := scanValues(rows, expected)
				if s.NoError(err) {
					s.Equal([][]interface{}{expected}, v)
				}
			}
			s.NoError(rows.Close())
		}
		s.NoError(st.Close())
		_, err = st.Query(tc.args[0]...)
		s.EqualError(err, "sql: statement is closed")
	}
}

func (s *stmtSuite) TestExec() {
	testCases := []struct {
		query  string
		query2 string
		args   [][]interface{}
	}{
		{
			"INSERT INTO data (i64) VALUES (?)",
			"SELECT i64 FROM data WHERE i64=?",
			[][]interface{}{{int64(11)}},
		},
		{
			"INSERT INTO data (i64) VALUES (?)",
			"SELECT i64 FROM data WHERE i64=?",
			[][]interface{}{{int64(12)}, {int64(13)}},
		},
	}

	for _, tc := range testCases {
		st, err := s.conn.Prepare(tc.query)
		if !s.NoError(err) {
			continue
		}
		for _, args := range tc.args {
			result, err := st.Exec(args...)
			if !s.NoError(err) {
				continue
			}
			s.NotNil(result)
			rows, err := s.conn.Query(tc.query2, args...)
			if !s.NoError(err) {
				continue
			}
			v, err := scanValues(rows, args)
			if s.NoError(err) {
				s.Equal([][]interface{}{args}, v)
			}
			s.NoError(rows.Close())
		}
		s.NoError(st.Close())
		_, err = st.Exec(tc.args[0]...)
		s.EqualError(err, "sql: statement is closed")
	}

}

func (s *stmtSuite) TestExecMulti() {
	testCases := []struct {
		insertQuery string
		exec1       int64
		exec2       int64
		query1      string
		query2      string
		expected    [][]interface{}
	}{
		{
			"INSERT INTO data (i64) VALUES (?)",
			21,
			22,
			"SELECT i64 FROM data WHERE i64=21",
			"SELECT i64 FROM data WHERE i64>20",
			[][]interface{}{{int64(21)}, {int64(22)}},
		},
		{
			"INSERT\nINTO\ndata\n(\ni64\n)\nVALUES\n(\n?\n)",
			23,
			24,
			"SELECT i64 FROM data WHERE i64=23",
			"SELECT i64 FROM data WHERE i64>22",
			[][]interface{}{{int64(23)}, {int64(24)}},
		},
	}

	for _, tc := range testCases {
		require := s.Require()
		tx, err := s.conn.Begin()
		require.NoError(err)
		st, err := tx.Prepare(tc.insertQuery)
		require.NoError(err)
		_, err = st.Exec(tc.exec1)
		require.NoError(err)
		_, err = st.Exec(tc.exec2)
		require.NoError(err)
		rows, err := s.conn.Query(tc.query1)
		require.NoError(err)
		s.False(rows.Next())
		s.NoError(rows.Close())
		require.NoError(tx.Commit())
		s.NoError(st.Close())
		rows, err = s.conn.Query(tc.query2)
		require.NoError(err)
		v, err := scanValues(rows, tc.expected[0])
		s.NoError(rows.Close())
		require.NoError(err)
		s.Equal(tc.expected, v)
	}
}

func (s *stmtSuite) TestExecMultiRollback() {
	require := s.Require()
	tx, err := s.conn.Begin()
	require.NoError(err)
	st, err := tx.Prepare("INSERT INTO data (i64) VALUES (?)")
	require.NoError(err)
	_, err = st.Exec(31)
	require.NoError(err)
	_, err = st.Exec(32)
	require.NoError(err)
	rows, err := s.conn.Query("SELECT i64 FROM data WHERE i64=31")
	s.NoError(err)
	s.False(rows.Next())
	s.NoError(rows.Close())
	require.NoError(tx.Rollback())
	s.NoError(st.Close())
	rows, err = s.conn.Query("SELECT i64 FROM data WHERE i64>30")
	require.NoError(err)
	s.False(rows.Next())
	s.NoError(rows.Close())
}

func (s *stmtSuite) TestExecMultiInterrupt() {
	require := s.Require()
	tx, err := s.conn.Begin()
	require.NoError(err)
	st, err := tx.Prepare("INSERT INTO data (i64) VALUES (?)")
	require.NoError(err)
	st2, err := tx.Prepare("INSERT INTO data (i64) VALUES (?)")
	require.NoError(err)
	_, err = st.Exec(31)
	require.NoError(err)
	_, err = st.Exec(32)
	require.NoError(err)
	rows, err := s.conn.Query("SELECT i64 FROM data WHERE i64=31")
	s.NoError(err)
	s.False(rows.Next())
	s.NoError(rows.Close())
	require.NoError(st.Close())
	require.NoError(tx.Commit())
	require.NoError(st2.Close())
	rows, err = s.conn.Query("SELECT i64 FROM data WHERE i64>30")
	require.NoError(err)
	s.False(rows.Next())
	s.NoError(rows.Close())
}

func (s *stmtSuite) TestFixDoubleInterpolateInStmt() {
	require := s.Require()
	tx, err := s.conn.Begin()
	require.NoError(err)
	st, err := tx.Prepare("INSERT INTO data (s, s2) VALUES (?, ?)")
	require.NoError(err)
	args := []interface{}{"'", "?"}
	_, err = st.Exec(args...)
	require.NoError(err)
	require.NoError(tx.Commit())
	require.NoError(st.Close())
	rows, err := s.conn.Query("SELECT s, s2 FROM data WHERE s='\\'' AND s2='?'")
	require.NoError(err)
	v, err := scanValues(rows, args)
	if s.NoError(err) {
		s.Equal([][]interface{}{args}, v)
	}
	s.NoError(rows.Close())
}

func TestStmt(t *testing.T) {
	suite.Run(t, new(stmtSuite))
}
