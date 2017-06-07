package neo

import (
	"context"
	"database/sql"
	"fmt"
	"notifier/libs/logging"

	log "github.com/Sirupsen/logrus"

	neoDriver "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"github.com/pkg/errors"
	"time"
)

var gLogger = logging.WithPackage("neo")

type Client struct {
	driver          neoDriver.DriverPool
	retriesInterval time.Duration
	retriesNum      int
}

type Connection struct {
	conn neoDriver.Conn
}

func (c *Client) GetConn() (*Connection, error) {
	underlingConn, err := c.driver.OpenPool()
	if err != nil {
		return nil, errors.Wrap(err, "cannot get connection from pool")
	}
	conn := &Connection{underlingConn}
	return conn, nil
}

//type TransactionFunc func(conn Connection) error
//
//func (c *Client) Transaction(needRetry bool, work TransactionFunc) error{
//
//}

func (c *Connection) Query(ctx context.Context, query string, params map[string]interface{}) ([][]interface{}, error) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithFields(log.Fields{"query": query, "params": params}).Debug("Quering")
	rows, _, _, err := c.conn.QueryNeoAll(query, params)
	if err != nil {
		return nil, errors.Wrap(err, "query failed")
	}
	return rows, nil
}

func (c *Connection) QueryOne(ctx context.Context, query string, params map[string]interface{}) ([]interface{}, error) {
	rows, err := c.Query(ctx, query, params)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, sql.ErrNoRows
	}
	return rows[0], nil
}

func (c *Connection) Exec(ctx context.Context, query string, params map[string]interface{}) error {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithFields(log.Fields{"query": query, "params": params}).Debug("Executing")
	_, err := c.conn.ExecNeo(query, params)
	if err != nil {
		return errors.Wrap(err, "execution failed")
	}
	return nil
}

func (c *Connection) Close(ctx context.Context) {
	err := c.conn.Close()
	if err != nil {
		logger := logging.FromContextAndBase(ctx, gLogger)
		logger.Warnf("Connection closing failed: %s", err)
	}
}

func buildConnectionStr(host string, port int, user, password string, timeout int) string {
	uri := fmt.Sprintf("bolt://%s:%s@%s:%d?timeout=%d", user, password, host, port, timeout)
	return uri
}

func NewClient(host string, port int, user, password string, timeout, poolSize, retriesNum, retriesInterval int) (
	*Client, error) {

	connStr := buildConnectionStr(host, port, user, password, timeout)
	gLogger.WithField("url", connStr).Info("Connecting to neo4j")
	pool, err := neoDriver.NewDriverPool(connStr, poolSize)
	if err == nil {
		var conn neoDriver.Conn
		conn, err = pool.OpenPool()
		if err == nil {
			defer conn.Close()
		}
	}
	if err != nil {
		return nil, errors.Wrap(err, "connection failed")
	}
	client := &Client{
		driver:          pool,
		retriesNum:      retriesNum,
		retriesInterval: time.Duration(retriesInterval) * time.Millisecond,
	}
	return client, nil
}
