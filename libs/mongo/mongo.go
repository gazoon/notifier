package mongo

import (
	"context"
	"notifier/libs/logging"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"gopkg.in/mgo.v2"
)

var (
	gLogger         = logging.WithPackage("mongo")
	DuplicateKeyErr = errors.New("mongo: duplication key")
)

const (
	duplicateKeyCode = 11000
)

type Client struct {
	session         *mgo.Session
	db              *mgo.Database
	collection      *mgo.Collection
	retriesInterval time.Duration
	retriesNum      int
}

func NewClient(database, collection, user, password, host string, port, timeout, poolSize,
	retriesNum, retriesInterval int) (*Client, error) {

	info := &mgo.DialInfo{
		Addrs:     []string{host + ":" + strconv.Itoa(port)},
		Database:  database,
		Username:  user,
		Password:  password,
		Timeout:   time.Duration(timeout) * time.Second,
		PoolLimit: poolSize,
	}
	gLogger.Infof("Connecting. Dial info: %+v", info)
	session, err := mgo.DialWithInfo(info)
	if err != nil {
		return nil, errors.Wrap(err, "dialing failed")
	}
	session.SetSafe(&mgo.Safe{WMode: "majority", J: true, WTimeout: timeout * 1000})
	session.SetMode(mgo.Eventual, true)
	db := session.DB("")
	return &Client{
		session:         session,
		db:              db,
		collection:      db.C(collection),
		retriesNum:      retriesNum,
		retriesInterval: time.Duration(retriesInterval) * time.Millisecond,
	}, nil
}

func (c *Client) Upsert(ctx context.Context, needRetry bool, query, update interface{}) error {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithFields(log.Fields{"query": query, "update": update}).Debug("Upserting document")
	err := c.withRetriesLoop(ctx, needRetry, func() error {
		_, err := c.collection.Upsert(query, update)
		return err
	})
	if isDuplicationErr(err) {
		return DuplicateKeyErr
	}
	return errors.Wrap(err, "upsert failed")
}

func (c *Client) FindAndModify(ctx context.Context, needRetry bool, query interface{}, sort string, change mgo.Change,
	model interface{}) error {

	logger := logging.FromContextAndBase(ctx, gLogger)
	q := c.collection.Find(query)
	if sort != "" {
		q.Sort(sort)
	}
	q.Limit(1)
	logger.WithFields(log.Fields{"query": query}).Debug("Find and modify document")
	err := c.withRetriesLoop(ctx, needRetry, func() error {
		_, err := q.Apply(change, model)
		return err
	})
	if err == mgo.ErrNotFound {
		return err
	}
	return errors.Wrap(err, "findAndModify failed")
}

func (c *Client) Update(ctx context.Context, needRetry bool, query, update interface{}) error {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithFields(log.Fields{"query": query, "update": update}).Debug("Updating document")
	err := c.withRetriesLoop(ctx, needRetry, func() error {
		return c.collection.Update(query, update)
	})
	if err == mgo.ErrNotFound {
		return err
	}
	return errors.Wrap(err, "update failed")
}

func (c *Client) Insert(ctx context.Context, needRetry bool, doc interface{}) error {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("document", doc).Debug("inserting document")
	err := c.withRetriesLoop(ctx, needRetry, func() error {
		return c.collection.Insert(doc)
	})
	if isDuplicationErr(err) {
		return DuplicateKeyErr
	}
	return errors.Wrap(err, "insert failed")
}

func (c *Client) Remove(ctx context.Context, query interface{}) (int, error) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("query", query).Debug("removing documents")
	var info *mgo.ChangeInfo
	err := c.withRetriesLoop(ctx, true, func() error {
		var err error
		info, err = c.collection.RemoveAll(query)
		return err
	})
	removed := 0
	if info != nil {
		removed = info.Removed
	}
	return removed, errors.Wrap(err, "removeAll failed")
}

func (c *Client) CreateIndex(unique, sparse bool, keys ...string) error {
	gLogger.WithFields(log.Fields{"index_key": keys, "unique": unique, "sparse": sparse}).Info("Creating index")
	err := c.collection.EnsureIndex(mgo.Index{Key: keys, Unique: unique, Sparse: sparse})
	return errors.Wrapf(err, "ensure index failed, key: %v", keys)
}

func (c *Client) withRetriesLoop(ctx context.Context, needRetry bool, work func() error) error {
	logger := logging.FromContextAndBase(ctx, gLogger)
	retriesNum := c.retriesNum
	if !needRetry {
		retriesNum = 0
	}
	for i := 0; ; i++ {
		err := work()
		if err == nil {
			return nil
		}
		if !isRetriable(err) || i >= retriesNum {
			return err
		}
		logger.Warnf("Query failed: %s. Retrying in %s, attempt %d", err, c.retriesInterval, i+1)
		time.Sleep(c.retriesInterval)
		c.session.Refresh()
	}
}

func isRetriable(err error) bool {
	if isDuplicationErr(err) || err == mgo.ErrNotFound {
		return false
	}
	return true
}

func isDuplicationErr(err error) bool {
	mgoErr, ok := err.(*mgo.LastError)
	return ok && mgoErr.Code == duplicateKeyCode
}
