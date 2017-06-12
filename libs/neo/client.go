package neo

import (
	"context"
	"database/sql"
	"fmt"
	"notifier/libs/logging"

	log "github.com/Sirupsen/logrus"

	neoDriver "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	grapherrors "github.com/johnnadratowski/golang-neo4j-bolt-driver/errors"
	graphmessages "github.com/johnnadratowski/golang-neo4j-bolt-driver/structures/messages"
	"github.com/pkg/errors"
	"time"
)

var (
	gLogger             = logging.WithPackage("neo")
	DuplicateKeyErr     = errors.New("neo: duplication key")
	IndexConstraintCode = "Neo.ClientError.Schema.ConstraintValidationFailed"
)

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

func (c *Client) retriesLoop(ctx context.Context, needRetry, isTransaction bool, work func(*Connection) error) error {
	logger := logging.FromContextAndBase(ctx, gLogger)
	retriesNum := c.retriesNum
	if !needRetry {
		retriesNum = 0
	}
	for i := 0; ; i++ {
		var conn *Connection
		var tx neoDriver.Tx
		var err error
		conn, err = c.GetConn()
		if err == nil {
			if isTransaction {
				logger.Info("Start a transaction")
				tx, err = conn.conn.Begin()
				err = errors.Wrap(err, "transaction begin failed")
			}
			if err == nil {
				err = work(conn)
				if isTransaction && err == nil {
					logger.Info("Commit transaction")
					err = errors.Wrap(tx.Commit(), "transaction commit failed")
				}
			}
		}
		if conn != nil {
			conn.Close(ctx)
		}
		if err == nil {
			return nil
		}
		if !isRetriable(err) || i >= retriesNum {
			return err
		}
		logger.Warnf("Transaction func failed: %s. Retrying in %s, attempt %d", err, c.retriesInterval, i+1)
		time.Sleep(c.retriesInterval)
	}
}

func (c *Client) transaction(ctx context.Context, needRetry bool, work func(*Connection) error) error {
	return c.retriesLoop(ctx, needRetry, true, work)
}

func (c *Client) Transaction(ctx context.Context, work func(*Connection) error) error {
	return c.transaction(ctx, false, work)
}

func (c *Client) TransactionRetry(ctx context.Context, work func(*Connection) error) error {
	return c.transaction(ctx, true, work)
}

func (c *Client) exec(ctx context.Context, needRetry bool, query string, params map[string]interface{}) error {
	return c.retriesLoop(ctx, needRetry, false, func(conn *Connection) error {
		return conn.Exec(ctx, query, params)
	})
}

func (c *Client) Exec(ctx context.Context, query string, params map[string]interface{}) error {
	return c.exec(ctx, false, query, params)
}

func (c *Client) ExecRetry(ctx context.Context, query string, params map[string]interface{}) error {
	return c.exec(ctx, true, query, params)
}

func (c *Client) query(ctx context.Context, needRetry bool, query string, params map[string]interface{}) (
	[][]interface{}, error) {

	var result [][]interface{}
	err := c.retriesLoop(ctx, needRetry, false, func(conn *Connection) error {
		var err error
		result, err = conn.Query(ctx, query, params)
		return err
	})
	return result, err
}

func (c *Client) Query(ctx context.Context, query string, params map[string]interface{}) ([][]interface{}, error) {
	return c.query(ctx, false, query, params)
}

func (c *Client) QueryRetry(ctx context.Context, query string, params map[string]interface{}) ([][]interface{}, error) {
	return c.query(ctx, true, query, params)
}

func (c *Client) queryOne(ctx context.Context, needRetry bool, query string, params map[string]interface{}) (
	[]interface{}, error) {

	var result []interface{}
	err := c.retriesLoop(ctx, needRetry, false, func(conn *Connection) error {
		var err error
		result, err = conn.QueryOne(ctx, query, params)
		return err
	})
	return result, err
}

func (c *Client) QueryOne(ctx context.Context, query string, params map[string]interface{}) ([]interface{}, error) {
	return c.queryOne(ctx, false, query, params)
}

func (c *Client) QueryOneRetry(ctx context.Context, query string, params map[string]interface{}) ([]interface{}, error) {
	return c.queryOne(ctx, true, query, params)
}

func (c *Connection) Query(ctx context.Context, query string, params map[string]interface{}) ([][]interface{}, error) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithFields(log.Fields{"query": query, "params": params}).Info("Quering")
	rows, _, _, err := c.conn.QueryNeoAll(query, params)
	return rows, errors.Wrap(err, "query failed")
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
	logger.WithFields(log.Fields{"query": query, "params": params}).Info("Executing")
	_, err := c.conn.ExecNeo(query, params)
	if isIndexConstraintError(err) {
		return DuplicateKeyErr
	}
	return errors.Wrap(err, "execution failed")
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

func isRetriable(err error) bool {
	if err == nil {
		return false
	}
	return err != DuplicateKeyErr && err != sql.ErrNoRows
}

func isIndexConstraintError(err error) bool {
	gerr, ok := err.(*grapherrors.Error)
	if !ok {
		return false
	}
	msg, ok := gerr.InnerMost().(graphmessages.FailureMessage)
	if !ok {
		return false
	}
	errCode, ok := msg.Metadata["code"].(string)
	if !ok {
		return false
	}
	return errCode == IndexConstraintCode
}
