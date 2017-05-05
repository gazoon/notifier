package neo

import (
	"context"
	"database/sql"
	"fmt"
	"notifier/logging"

	neoDriver "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"github.com/pkg/errors"
)

var gLogger = logging.WithPackage("neo")

type NeoConn interface {
	Query(ctx context.Context, query string, params map[string]interface{}) ([][]interface{}, error)
	QueryOne(ctx context.Context, query string, params map[string]interface{}) ([]interface{}, error)
	Exec(ctx context.Context, query string, params map[string]interface{}) error
}

type NeoClient interface {
	GetConn() (NeoConn, error)
}

type client struct {
	driver neoDriver.DriverPool
}

type connection struct {
	conn neoDriver.Conn
}

func (c *client) GetConn() (NeoConn, error) {
	underlingConn, err := c.driver.OpenPool()
	if err != nil {
		return nil, errors.WithMessage(err, "cannot get connection from pool")
	}
	conn := &connection{underlingConn}
	return conn, err
}

func (c *connection) Query(ctx context.Context, query string, params map[string]interface{}) ([][]interface{}, error) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("query", query).Debug("Quering")
	rows, _, _, err := c.conn.QueryNeoAll(query, params)
	if err != nil {
		return nil, errors.WithMessage(err, "query failed")
	}
	return rows, nil
}

func (c *connection) QueryOne(ctx context.Context, query string, params map[string]interface{}) ([]interface{}, error) {
	rows, err := c.Query(ctx, query, params)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, sql.ErrNoRows
	}
	return rows[0], nil
}

func (c *connection) Exec(ctx context.Context, query string, params map[string]interface{}) error {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("query", query).Debug("Executing")
	_, err := c.conn.ExecNeo(query, params)
	if err != nil {
		return errors.WithMessage(err, "execution failed")
	}
	return nil
}

func buildConnectionStr(host string, port int, user, password string, timeout int) string {
	uri := fmt.Sprintf("bolt://%s:%s@%s:%d?timeout=%d", user, password, host, port, timeout)
	return uri
}

func NewClient(host string, port int, user, password string, timeout, poolSize int) (NeoClient, error) {
	connStr := buildConnectionStr(host, port, user, password, timeout)
	gLogger.WithField("url", connStr).Info("Connecting to neo")
	pool, err := neoDriver.NewDriverPool(connStr, poolSize)
	if err == nil {
		var conn neoDriver.Conn
		conn, err = pool.OpenPool()
		if err == nil {
			defer conn.Close()
		}
	}
	if err != nil {
		return nil, errors.WithMessage(err, "cannot create driver")
	}
	return &client{pool}, nil
}
