package clickhouse

import (
	"bytes"
	"context"
	"database/sql/driver"
	"regexp"
	"strings"
)

var (
	splitInsertRe = regexp.MustCompile(`(.+\s+VALUES)\s*(\(.+\))`)
)

type stmt struct {
	c         *conn
	prefix    string
	pattern   string
	index     []int
	batchMode bool
	args      [][]driver.Value
}

func newStmt(query string) *stmt {
	s := &stmt{pattern: query}
	index := splitInsertRe.FindStringSubmatchIndex(strings.ToUpper(query))
	if len(index) == 6 {
		s.prefix = query[index[2]:index[3]]
		s.pattern = query[index[4]:index[5]]
		s.batchMode = true
	}
	s.index = placeholders(s.pattern)
	if len(s.index) == 0 {
		s.batchMode = false
	}
	return s
}

// Query executes a query that may return rows, such as a SELECT
func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.query(context.Background(), args)
}

// Exec executes a query that doesn't return rows, such as an INSERT
func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.exec(context.Background(), args)
}

// Close closes the statement.
func (s *stmt) Close() error {
	if s.c != nil {
		// make close idempotent
		s.c.closeStmt(s)
		s.clean()
	}
	return nil
}

// NumInput returns the number of placeholder parameters.
func (s *stmt) NumInput() int {
	return len(s.index)
}

func (s *stmt) query(ctx context.Context, args []driver.Value) (driver.Rows, error) {
	q, err := interpolateParams2(s.pattern, args, s.index)
	if err != nil {
		return nil, err
	}
	return s.c.query(ctx, s.prefix+q, nil)
}

func (s *stmt) exec(ctx context.Context, args []driver.Value) (driver.Result, error) {
	if s.batchMode {
		s.args = append(s.args, args)
		return emptyResult, nil
	}
	q, err := interpolateParams2(s.pattern, args, s.index)
	if err != nil {
		return nil, err
	}
	return s.c.exec(ctx, s.prefix+q, nil)
}

func (s *stmt) commit(ctx context.Context) error {
	if s.c == nil {
		// statement has been closed
		return nil
	}
	if len(s.args) == 0 {
		return nil
	}
	buf := bytes.NewBufferString(s.prefix)
	var (
		p   string
		err error
	)
	for i, args := range s.args {
		if i > 0 {
			buf.WriteString(", ")
		}
		if p, err = interpolateParams(s.pattern, args); err != nil {
			return err
		}
		buf.WriteString(p)
	}
	_, err = s.c.exec(ctx, buf.String(), nil)
	s.args = s.args[0:0]
	return err
}

func (s *stmt) clean() {
	s.c = nil
}
