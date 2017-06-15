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

func (c *Client) Find(ctx context.Context, query interface{}, sort string, limit int, model interface{}) error {
	logger := logging.FromContextAndBase(ctx, gLogger)
	q := c.collection.Find(query)
	if sort != "" {
		q = q.Sort(sort)
	}
	if limit > 0 {
		q = q.Limit(limit)
	}
	logger.WithFields(log.Fields{"query": query, "sort": sort, "limit": limit}).Info("Find all documents")
	return c.withRetriesLoop(ctx, true, func() error {
		err := q.All(model)
		return errors.Wrap(err, "find failed")
	})
}

func (c *Client) upsert(ctx context.Context, needRetry bool, query, update interface{}) error {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithFields(log.Fields{"query": query, "update": update}).Info("Upserting document")
	return c.withRetriesLoop(ctx, needRetry, func() error {
		_, err := c.collection.Upsert(query, update)
		if isDuplicationErr(err) {
			return DuplicateKeyErr
		}
		return errors.Wrap(err, "upsert failed")
	})
}

func (c *Client) Upsert(ctx context.Context, query, update interface{}) error {
	return c.upsert(ctx, false, query, update)
}

func (c *Client) UpsertRetry(ctx context.Context, query, update interface{}) error {
	return c.upsert(ctx, true, query, update)
}

func (c *Client) findAndModify(ctx context.Context, needRetry bool, query interface{}, sort string, change mgo.Change,
	model interface{}) error {

	logger := logging.FromContextAndBase(ctx, gLogger)
	q := c.collection.Find(query)
	if sort != "" {
		q.Sort(sort)
	}
	q.Limit(1)
	logger.WithFields(log.Fields{"query": query, "sort": sort}).Debug("Find and modify document")
	return c.withRetriesLoop(ctx, needRetry, func() error {
		_, err := q.Apply(change, model)
		if err == mgo.ErrNotFound {
			return err
		}
		return errors.Wrap(err, "findAndModify failed")
	})
}

func (c *Client) FindAndModify(ctx context.Context, query interface{}, sort string, change mgo.Change,
	model interface{}) error {

	return c.findAndModify(ctx, false, query, sort, change, model)
}

func (c *Client) FindAndModifyRetry(ctx context.Context, query interface{}, sort string, change mgo.Change,
	model interface{}) error {

	return c.findAndModify(ctx, true, query, sort, change, model)
}

func (c *Client) update(ctx context.Context, needRetry bool, query, update interface{}) error {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithFields(log.Fields{"query": query, "update": update}).Info("Updating document")
	err := c.withRetriesLoop(ctx, needRetry, func() error {
		err := c.collection.Update(query, update)
		if err == mgo.ErrNotFound {
			return err
		}
		return errors.Wrap(err, "update failed")
	})
	return err
}

func (c *Client) Update(ctx context.Context, query, update interface{}) error {
	return c.update(ctx, false, query, update)
}

func (c *Client) UpdateRetry(ctx context.Context, query, update interface{}) error {
	return c.update(ctx, true, query, update)
}

func (c *Client) insert(ctx context.Context, needRetry bool, doc interface{}) error {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("document", doc).Info("inserting document")
	return c.withRetriesLoop(ctx, needRetry, func() error {
		err := c.collection.Insert(doc)
		if isDuplicationErr(err) {
			return DuplicateKeyErr
		}
		return errors.Wrap(err, "insert failed")
	})
}

func (c *Client) Insert(ctx context.Context, doc interface{}) error {
	return c.insert(ctx, false, doc)
}

func (c *Client) InsertRetry(ctx context.Context, doc interface{}) error {
	return c.insert(ctx, true, doc)
}

func (c *Client) Remove(ctx context.Context, query interface{}) (int, error) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("query", query).Info("removing documents")
	var info *mgo.ChangeInfo
	err := c.withRetriesLoop(ctx, true, func() error {
		var err error
		info, err = c.collection.RemoveAll(query)
		return errors.Wrap(err, "removeAll failed")
	})
	removed := 0
	if info != nil {
		removed = info.Removed
	}
	return removed, err
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
	if err == DuplicateKeyErr || err == mgo.ErrNotFound {
		return false
	}
	return true
}

func isDuplicationErr(err error) bool {
	mgoErr, ok := err.(*mgo.LastError)
	return ok && mgoErr.Code == duplicateKeyCode
}
