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
