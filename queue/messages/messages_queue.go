package msgsqueue

import (
	"context"
	"notifier/logging"
	"notifier/models"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"

	"notifier/mongo"

	"notifier/queue"

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
	storage []*models.Message
	mx      sync.Mutex
	*queue.BaseConsumer
}

func NewInMemory() *InMemoryQueue {
	const fetchDelay = 50
	return &InMemoryQueue{BaseConsumer: queue.NewBaseConsumer(fetchDelay)}
}

func (mq *InMemoryQueue) GetNext() (*models.Message, bool) {
	var result *models.Message
	ok := mq.FetchLoop(func() bool {
		mq.mx.Lock()
		if len(mq.storage) == 0 {
			mq.mx.Unlock()
			return false
		}
		result = mq.storage[0]
		mq.storage = mq.storage[1:]
		mq.mx.Unlock()
		return true
	})
	return result, ok
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

type MongoQueue struct {
	client *mongo.Client
	*queue.BaseConsumer
}

type mongoMessage struct {
	models.Message `bson:",inline"`
	CreatedAt      time.Time  `bson:"created_at"`
	DispatchedAt   *time.Time `bson:"dispatched_at,omitempty"`
}

func (m mongoMessage) String() string {
	return logging.ObjToString(&m)
}

func NewMongoQueue(database, user, password, host string, port, timeout, poolSize, fetchDelay int) (*MongoQueue, error) {
	client, err := mongo.NewClient(database, mongoCollection, user, password, host, port, timeout, poolSize)
	if err != nil {
		return nil, err
	}
	return &MongoQueue{client: client, BaseConsumer: queue.NewBaseConsumer(fetchDelay)}, nil
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

func (mq *MongoQueue) GetNext() (*models.Message, bool) {
	var result *models.Message
	ok := mq.FetchLoop(func() bool {
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
			return false
		}
		if len(doc.Msgs) == 0 {
			gLogger.Error("Received document without messages")
			return false
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
			return false
		}
		result = &msg.Message
		return true
	})
	return result, ok
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
