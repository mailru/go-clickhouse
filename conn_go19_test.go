//go:build go1.9
// +build go1.9

package clickhouse

func (s *connSuite) TestExecBuild19() {
	testCases := []struct {
		query  string
		query2 string
		args   []interface{}
	}{
		{
			"INSERT INTO data (u64) VALUES (?)",
			"SELECT u64 FROM data WHERE u64=?",
			[]interface{}{uint64(maxAllowedUInt64 - 1)},
		},
		{
			"INSERT INTO data (u64) VALUES (?)",
			"SELECT u64 FROM data WHERE u64=?",
			[]interface{}{uint64(maxAllowedUInt64 * 2)},
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

func (s *connSuite) TestQuotedStrings() {
	testCases := []struct {
		query, expected1, expected2 string
	}{
		{
			`SELECT '"foo" foo', 'bar'`, `"foo" foo`, "bar",
		},
		{
			`SELECT 'bar', '"foo" foo'`, "bar", `"foo" foo`,
		},
	}
	for _, tc := range testCases {
		var actual1, actual2 string
		err := s.conn.QueryRow(tc.query).Scan(&actual1, &actual2)
		if !s.NoError(err) {
			continue
		}
		s.Equal(tc.expected1, actual1)
		s.Equal(tc.expected2, actual2)
	}
}
