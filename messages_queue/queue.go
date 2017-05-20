package msgsqueue

import (
	"context"
	"notifier/logging"
	"notifier/models"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/Sirupsen/logrus"

	"notifier/mongo"

	"github.com/pkg/errors"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	MAX_PROCESSING_TIME = 5 * time.Second
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
	client     *mongo.Client
	readClosed int32
	fetchDelay time.Duration
}

type mongoMessage struct {
	models.Message `bson:",inline"`
	CreatedAt      time.Time  `bson:"created_at"`
	DispatchedAt   *time.Time `bson:"dispatched_at,omitempty"`
}

func (m mongoMessage) String() string {
	return logging.ObjToString(&m)
}

func NewMongoQueue(database, user, password, host string, port, timeout, poolSize int) (*MongoQueue, error) {
	client, err := mongo.NewClient(database, mongoCollection, user, password, host, port, timeout, poolSize)
	if err != nil {
		return nil, errors.Wrap(err, "mongo initialization failed")
	}
	return &MongoQueue{client: client, fetchDelay: time.Millisecond * 50}, nil
}

func (mq *MongoQueue) Put(ctx context.Context, msg *models.Message) error {
	err := mq.client.Upsert(ctx,
		bson.M{"chat_id": msg.Chat.ID},
		bson.M{
			"$set": bson.M{"chat_id": msg.Chat.ID},
			"$push": bson.M{"msgs": &mongoMessage{
				Message:      *msg,
				CreatedAt:    time.Now(),
				DispatchedAt: nil,
			}},
		})
	return err
}

func (mq *MongoQueue) StopGivingMsgs() {
	atomic.StoreInt32(&mq.readClosed, 1)
}

func (mq *MongoQueue) GetNext() (*models.Message, bool) {
	for {
		if atomic.LoadInt32(&mq.readClosed) == 1 {
			return nil, false
		}

		var doc struct {
			Msgs []*mongoMessage `bson:"msgs"`
		}
		dispatchAtKey := "msgs.0.dispatched_at"
		currentTime := time.Now()
		err := mq.client.FindAndModify(context.Background(),
			bson.M{
				"msgs": bson.M{"$gt": []interface{}{}},
				"$or": []bson.M{
					{dispatchAtKey: bson.M{"$exists": false}},
					{dispatchAtKey: bson.M{"$lt": currentTime.Add(-MAX_PROCESSING_TIME)}},
				}},
			"msgs.0.created_at",
			mgo.Change{Update: bson.M{"$set": bson.M{dispatchAtKey: currentTime}}},
			&doc)
		if err != nil {
			if err != mgo.ErrNotFound {
				gLogger.Errorf("Cannot fetch document from mongo: %s", err)
			}
			time.Sleep(mq.fetchDelay)
			continue
		}
		if len(doc.Msgs) == 0 {
			gLogger.Error("Received document without messages")
			time.Sleep(mq.fetchDelay)
			continue
		}
		msg := doc.Msgs[0]
		logger := logging.WithRequestIDAndBase(msg.RequestID, gLogger)
		logger.WithField("mongo_message", msg).Info("Message retrieved from mongo")
		if msg.DispatchedAt != nil {
			logger.WithFields(log.Fields{"message_id": msg.ID, "chat_id": msg.Chat.ID}).
				Warn("Message already has been dispatched, removing")
			err := mq.removeMessage(context.Background(), msg.Chat.ID, msg.ID)
			if err != nil {
				logger.Errorf("Cannot remove stuck message: %s", err)
			}
			time.Sleep(mq.fetchDelay)
			continue
		}
		return &msg.Message, true
	}
}

func (mq *MongoQueue) removeMessage(ctx context.Context, chatID, messageID int) error {
	err := mq.client.Update(ctx,
		bson.M{"chat_id": chatID, "msgs.0.message_id": messageID},
		bson.M{"$pop": bson.M{"msgs": -1}},
	)
	return err
}

func (mq *MongoQueue) Remove(ctx context.Context, msg *models.Message) error {
	return mq.removeMessage(ctx, msg.Chat.ID, msg.ID)
}
