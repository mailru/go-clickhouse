package clickhouse

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// conn implements an interface sql.Conn
type conn struct {
	url       *url.URL
	location  *time.Location
	transport *http.Transport
	cancel    context.CancelFunc
	txCtx     context.Context
	stmts     []*stmt
	logger    *log.Logger
}

func newConn(cfg *Config) *conn {
	var logger *log.Logger
	if cfg.Debug {
		logger = log.New(os.Stderr, "clickhouse: ", log.LstdFlags)
	}
	c := &conn{
		url:      cfg.url(map[string]string{"default_format": "TSVWithNamesAndTypes"}, false),
		location: cfg.Location,
		transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   cfg.Timeout,
				KeepAlive: cfg.IdleTimeout,
				DualStack: true,
			}).DialContext,
			MaxIdleConns:          1,
			IdleConnTimeout:       cfg.IdleTimeout,
			ResponseHeaderTimeout: cfg.ReadTimeout,
		},
		logger: logger,
	}
	c.log("new connection", c.url.Scheme, c.url.Host, c.url.Path)
	return c
}

func (c *conn) log(msg ...interface{}) {
	if c.logger != nil {
		c.logger.Println(msg...)
	}
}

// Prepare returns a prepared statement, bound to this connection.
func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.prepare(query)
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
func (c *conn) Close() error {
	c.log("close connection", c.url.Scheme, c.url.Host, c.url.Path)
	cancel := c.cancel
	transport := c.transport
	c.transport = nil
	c.cancel = nil

	if cancel != nil {
		cancel()
	}
	if transport != nil {
		transport.CloseIdleConnections()
	}
	return nil
}

// Begin starts and returns a new transaction.
func (c *conn) Begin() (driver.Tx, error) {
	return c.beginTx(context.Background())
}

// Commit applies prepared statement if it exists
func (c *conn) Commit() (err error) {
	if c.txCtx == nil {
		return sql.ErrTxDone
	}
	stmts := c.stmts
	c.stmts = stmts[:0]

	if len(stmts) == 0 {
		return nil
	}
	for _, stmt := range stmts {
		c.log("commit statement: ", stmt.prefix, stmt.pattern)
		if err == nil {
			if err = stmt.commit(c.txCtx); err != nil {
				break
			}
		}
		stmt.clean()
	}
	c.txCtx = nil
	return
}

// Rollback cleans prepared statement
func (c *conn) Rollback() error {
	if c.txCtx == nil {
		return sql.ErrTxDone
	}
	c.txCtx = nil
	stmts := c.stmts
	c.stmts = stmts[:0]

	if len(stmts) == 0 {
		return sql.ErrTxDone
	}
	for _, stmt := range stmts {
		c.log("discard statement: ", stmt.prefix, stmt.pattern)
		stmt.clean()
	}
	c.txCtx = nil
	return nil
}

// Exec implements the driver.Execer
func (c *conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	return c.exec(context.Background(), query, args)
}

// Query implements the driver.Queryer
func (c *conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return c.query(context.Background(), query, args)
}

func (c *conn) beginTx(ctx context.Context) (driver.Tx, error) {
	c.txCtx = ctx
	return c, nil
}

func (c *conn) query(ctx context.Context, query string, args []driver.Value) (driver.Rows, error) {
	req, err := c.buildRequest(query, args, true)
	if err != nil {
		return nil, err
	}
	body, err := c.doRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	return newTextRows(body, c.location)
}

func (c *conn) exec(ctx context.Context, query string, args []driver.Value) (driver.Result, error) {
	req, err := c.buildRequest(query, args, false)
	if err != nil {
		return nil, err
	}
	_, err = c.doRequest(ctx, req)
	return emptyResult, err
}

func (c *conn) doRequest(ctx context.Context, req *http.Request) ([]byte, error) {
	ctx, cancel := context.WithCancel(ctx)
	transport := c.transport
	c.cancel = cancel

	defer func() {
		c.cancel = nil
	}()

	if transport == nil {
		return nil, driver.ErrBadConn
	}

	req = req.WithContext(ctx)
	resp, err := transport.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		msg, err := readResponse(resp)
		if err != nil {
			return nil, err
		}
		return nil, newError(string(msg))
	}
	return readResponse(resp)
}

func (c *conn) buildRequest(query string, params []driver.Value, readonly bool) (*http.Request, error) {
	query, err := interpolateParams(query, params)
	if err != nil {
		return nil, err
	}
	var method string
	if readonly {
		method = http.MethodGet
	} else {
		method = http.MethodPost
	}
	c.log("query: ", query)
	return http.NewRequest(method, c.url.String(), strings.NewReader(query))
}

func (c *conn) prepare(query string) (*stmt, error) {
	c.log("new statement: ", query)
	s := newStmt(query)
	s.c = c
	if c.txCtx == nil {
		s.batchMode = false
	}
	if s.batchMode {
		c.stmts = append(c.stmts, s)
	}
	return s, nil
}

func (c *conn) closeStmt(s *stmt) {
	c.log("close statement: ", s.prefix, s.pattern)
	if len(c.stmts) == 0 {
		return
	}
	newstmts := make([]*stmt, len(c.stmts))
	j := 0
	for _, st := range c.stmts {
		if st != s {
			newstmts[j] = st
			j++
		}
	}
	c.stmts = newstmts[:j]
}
