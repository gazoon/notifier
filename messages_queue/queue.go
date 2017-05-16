package msgsqueue

import (
	"context"
	"notifier/logging"
	"notifier/models"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	gLogger         = logging.WithPackage("incoming_queue")
	mongoCollection = "messages"
)

type Producer interface {
	Put(ctx context.Context, msg *models.Message) error
}

type Consumer interface {
	GetNext() (*models.Message, bool)
	Remove(ctx context.Context, msg *models.Message) error
	StopGivingMsgs()
}

type InMemoryQueue struct {
	storage    []*models.Message
	readClosed int32
	mx         sync.Mutex
}

func NewInMemory() *InMemoryQueue {
	return &InMemoryQueue{}
}

func (mq *InMemoryQueue) GetNext() (*models.Message, bool) {
	for {
		if atomic.LoadInt32(&mq.readClosed) == 1 {
			return nil, false
		}
		mq.mx.Lock()
		if len(mq.storage) == 0 {
			mq.mx.Unlock()
			const fetch_delay = 10
			//gLogger.Debugf("List with messages is empty, wait %d milliseconds before the next attempt", fetch_delay)
			time.Sleep(time.Duration(fetch_delay) * time.Millisecond)
			continue
		}
		msg := mq.storage[0]
		mq.storage = mq.storage[1:]
		mq.mx.Unlock()
		return msg, true
	}
}

func (mq *InMemoryQueue) Put(ctx context.Context, msg *models.Message) error {
	mq.mx.Lock()
	defer mq.mx.Unlock()
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.Info("Append msg to the tail of the list")
	mq.storage = append(mq.storage, msg)
	return nil
}
func (mq *InMemoryQueue) Remove(ctx context.Context, msg *models.Message) error {
	return nil
}

func (mq *InMemoryQueue) StopGivingMsgs() {
	atomic.StoreInt32(&mq.readClosed, 1)
}

type MongoQueue struct {
	session    *mgo.Session
	db         *mgo.Database
	collection *mgo.Collection
	readClosed int32
}

func NewMongoQueue(database, user, password, host string, port, timeout, poolSize int) (*MongoQueue, error) {
	session, err := mgo.DialWithInfo(&mgo.DialInfo{
		Addrs:     []string{host + ":" + strconv.Itoa(port)},
		Database:  database,
		Username:  user,
		Password:  password,
		Timeout:   time.Duration(timeout) * time.Second,
		PoolLimit: poolSize,
	})
	if err != nil {
		return nil, errors.Wrap(err, "dialing failed")
	}
	session.SetSafe(&mgo.Safe{WMode: "majority", J: true, WTimeout: timeout * 1000})
	session.SetMode(mgo.Eventual, true)
	db := session.DB("")
	collection := db.C(mongoCollection)
	return &MongoQueue{session: session, db: db, collection: collection}, nil
}

func (mq *MongoQueue) Put(ctx context.Context, msg *models.Message) error {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.Info("Inserting new notification in the mongo")
	_, err := mq.collection.Upsert(bson.M{"chat_id": msg.Chat.ID},
		bson.M{
			"$set": bson.M{"chat_id": msg.Chat.ID},
			"$push": bson.M{"msgs": bson.M{
				"created_at":    time.Now(),
				"dispatched_at": nil,
				"message_id":    msg.ID,
				"payload":       msg,
			}},
		})
	if err != nil {
		return errors.Wrap(err, "inserting failed")
	}
	return nil
}

func (mq *MongoQueue) StopGivingMsgs() {
	atomic.StoreInt32(&mq.readClosed, 1)
}

func (mq *MongoQueue) GetNext() (*models.Message, bool) {
	gLogger.Info("Inserting new notification in the mongo")
	for {
		if atomic.LoadInt32(&mq.readClosed) == 1 {
			return nil, false
		}
		model := &models.Message{}
		_, err := mq.collection.Find(bson.M{"readyat": bson.M{"$lt": time.Now()}}).Sort("readyat").Limit(1).Apply(
			mgo.Change{Remove: true}, model)
		if err != nil {
			if err != mgo.ErrNotFound {
				gLogger.Errorf("Cannot fetch record from mongo: %s", err)
			}
			const fetch_delay = 10
			time.Sleep(time.Duration(fetch_delay) * time.Millisecond)
			continue
		}
		return model, true
	}
}

func (mq *MongoQueue) Remove(ctx context.Context, msg *models.Message) error {
	return nil
}
