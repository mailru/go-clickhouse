package clickhouse

import (
	"database/sql"
	"database/sql/driver"
)

func init() {
	sql.Register("chhttp", new(ChDriver))
}

// ChDriver implements sql.Driver interface
type ChDriver struct {
}

// Open returns new db connection
func (d *ChDriver) Open(dsn string) (driver.Conn, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return newConn(cfg), nil
}
