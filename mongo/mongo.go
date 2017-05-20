package mongo

import (
	"context"
	"notifier/logging"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"gopkg.in/mgo.v2"
)

var (
	gLogger = logging.WithPackage("mongo")
)

type Client struct {
	session    *mgo.Session
	db         *mgo.Database
	collection *mgo.Collection
}

func NewClient(database, collection, user, password, host string, port, timeout, poolSize int) (*Client, error) {
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
	return &Client{session: session, db: db, collection: db.C(collection)}, nil
}

func (c *Client) Upsert(ctx context.Context, query, update interface{}) error {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithFields(log.Fields{"query": query, "update": update}).Debug("Upserting document")
	_, err := c.collection.Upsert(query, update)
	if err != nil {
		return errors.Wrap(err, "upsert failed")
	}
	return nil
}

func (c *Client) FindAndModify(ctx context.Context, query interface{}, sort string, change mgo.Change,
	model interface{}) error {

	logger := logging.FromContextAndBase(ctx, gLogger)
	q := c.collection.Find(query)
	if sort != "" {
		q.Sort(sort)
	}
	q.Limit(1)
	logger.WithFields(log.Fields{"query": query}).Debug("Find and modify document")
	_, err := q.Apply(change, model)
	if err != nil {
		if err == mgo.ErrNotFound {
			return err
		}
		return errors.Wrap(err, "findAndModify failed")
	}
	return nil
}

func (c *Client) Update(ctx context.Context, query, update interface{}) error {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithFields(log.Fields{"query": query, "update": update}).Debug("Updating document")
	err := c.collection.Update(query, update)
	if err != nil {
		return errors.Wrap(err, "update failed")
	}
	return nil
}

func (c *Client) Insert(ctx context.Context, doc interface{}) error {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("document", doc).Debug("inserting document")
	err := c.collection.Insert(doc)
	if err != nil {
		return errors.Wrap(err, "insert failed")
	}
	return nil
}

func (c *Client) Remove(ctx context.Context, query interface{}) error {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("query", query).Debug("removing documents")
	_, err := c.collection.RemoveAll(query)
	if err != nil {
		return errors.Wrap(err, "removeAll failed")
	}
	return nil
}
