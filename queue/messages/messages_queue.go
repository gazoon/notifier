package msgsqueue

import (
	"context"
	"notifier/logging"
	"notifier/models"
	"sync"
	"time"

	"notifier/mongo"

	"notifier/queue"

	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	MAX_PROCESSING_TIME = 5 * time.Second
)

var (
	gLogger         = logging.WithPackage("incoming_queue")
	mongoCollection = "messages"
	DuplicateMsgErr = errors.New("message is already in the queue")
)

type Producer interface {
	Put(ctx context.Context, msg *models.Message) error
}

type Consumer interface {
	GetNext() (*models.Message, string, bool)
	FinishProcessing(ctx context.Context, processingID string) error
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

func NewMongoQueue(database, user, password, host string, port, timeout, poolSize, fetchDelay int) (*MongoQueue, error) {
	client, err := mongo.NewClient(database, mongoCollection, user, password, host, port, timeout, poolSize)
	if err != nil {
		return nil, err
	}
	return &MongoQueue{client: client, BaseConsumer: queue.NewBaseConsumer(fetchDelay)}, nil
}

func (mq *MongoQueue) Put(ctx context.Context, msg *models.Message) error {
	err := mq.client.Upsert(ctx,
		bson.M{"chat_id": msg.Chat.ID, "msgs.message_id": bson.M{"$ne": msg.ID}},
		bson.M{
			"$set":  bson.M{"chat_id": msg.Chat.ID},
			"$push": bson.M{"msgs": msg},
		})
	if err == mongo.DuplicateKeyErr {
		return DuplicateMsgErr
	}
	return err
}

func (mq *MongoQueue) GetNext() (*models.Message, string, bool) {
	var result *models.Message
	var processing_id string
	ok := mq.FetchLoop(func() bool {
		var doc struct {
			Msgs []*models.Message `bson:"msgs"`
		}
		currentTime := time.Now()
		processing_id = newProcessingID()
		err := mq.client.FindAndModify(context.Background(),
			bson.M{
				"msgs": bson.M{"$gt": []interface{}{}},
				"$or": []bson.M{
					{"processed_at": bson.M{"$exists": false}},
					{"processed_at": bson.M{"$lt": currentTime.Add(-MAX_PROCESSING_TIME)}},
				}},
			"msgs.0.created_at",
			mgo.Change{Update: bson.M{
				"$set": bson.M{"processed_at": currentTime, "processing_id": processing_id},
				"$pop": bson.M{"msgs": -1},
			}},
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
		result = doc.Msgs[0]
		return true
	})
	return result, processing_id, ok
}

func (mq *MongoQueue) FinishProcessing(ctx context.Context, processingID string) error {
	logger := logging.FromContextAndBase(ctx, gLogger)
	err := mq.client.Update(ctx,
		bson.M{"processing_id": processingID},
		bson.M{"$unset": bson.M{"processing_id": "", "processed_at": ""}},
	)
	if err == mgo.ErrNotFound {
		logger.WithField("processin_id", processingID).Warn("Processing with that id no longer exists")
		return nil
	}
	return err
}

func (mq *MongoQueue) PrepareIndexes() error {
	var err error

	err = mq.client.CreateIndex(false, false, "msgs.0.created_at")
	if err != nil {
		return errors.Wrap(err, "created_at index")
	}

	err = mq.client.CreateIndex(true, true, "processing_id")
	if err != nil {
		return errors.Wrap(err, "created_at index")
	}

	err = mq.client.CreateIndex(true, false, "chat_id")
	if err != nil {
		return errors.Wrap(err, "chat_id unique index")
	}

	return nil
}

func newProcessingID() string {
	return uuid.NewV4().String()
}
