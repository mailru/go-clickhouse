package clickhouse

import (
	"database/sql"
	"database/sql/driver"
)

func init() {
	sql.Register("chhttp", new(Driver))
}

// Driver implements sql.Driver interface
type Driver struct {
}

// Open returns new db connection
func (d *Driver) Open(dsn string) (driver.Conn, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return newConn(cfg), nil
}
