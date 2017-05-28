package notifqueue

import (
	"context"
	"notifier/models"
	"sync"

	"time"

	"notifier/logging"
	"notifier/mongo"

	"notifier/queue"

	"github.com/emirpasic/gods/sets/treeset"
	"github.com/pkg/errors"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	gLogger         = logging.WithPackage("notification_queue")
	mongoCollection = "notifications"
)

type Producer interface {
	Put(ctx context.Context, record *models.Notification) error
	Discard(ctx context.Context, user *models.User, chatID int) error
}

type Consumer interface {
	GetNext() (*models.Notification, bool)
	StopGivingMsgs()
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

func (mq *MongoQueue) Put(ctx context.Context, record *models.Notification) error {
	err := mq.client.Insert(ctx, record)
	if err == mongo.DuplicateKeyErr {
		logger := logging.FromContextAndBase(ctx, gLogger)
		logger.WithField("record", record).Warn("Notification document with that id already saved in the mongo")
		return nil
	}
	return err
}

func (mq *MongoQueue) Discard(ctx context.Context, user *models.User, chatID int) error {
	_, err := mq.client.Remove(ctx, bson.M{"user.id": user.ID, "chat_id": chatID})
	return err
}

func (mq *MongoQueue) GetNext() (*models.Notification, bool) {
	result := &models.Notification{}
	ok := mq.FetchLoop(func() bool {
		err := mq.client.FindAndModify(context.Background(),
			bson.M{"ready_at": bson.M{"$lt": time.Now()}},
			"ready_at",
			mgo.Change{Remove: true},
			result)
		if err != nil {
			if err != mgo.ErrNotFound {
				gLogger.Errorf("Cannot fetch documnt from mongo: %s", err)
			}
			return false
		}
		return true
	})
	return result, ok
}

func (mq *MongoQueue) PrepareIndexes() error {
	var err error

	err = mq.client.CreateIndex(false, false, "ready_at")
	if err != nil {
		return errors.Wrap(err, "ready_at index")
	}

	err = mq.client.CreateIndex(false, false, "user.id", "chat_id")
	if err != nil {
		return errors.Wrap(err, "user.id+chat_id index")
	}

	return nil
}

type InMemoryQueue struct {
	storage *treeset.Set
	*queue.BaseConsumer
	mx sync.Mutex
}

func NewInMemory() *InMemoryQueue {
	comparator := func(a, b interface{}) int {
		aRecord := a.(*models.Notification)
		bRecord := b.(*models.Notification)
		if aRecord == bRecord {
			return 0
		}
		if aRecord.ReadyAt.Before(bRecord.ReadyAt) {
			return -1
		}
		if aRecord.ReadyAt.After(bRecord.ReadyAt) {
			return 1
		}
		return -1
	}
	const fetchDelay = 50
	return &InMemoryQueue{
		storage:      treeset.NewWith(comparator),
		BaseConsumer: queue.NewBaseConsumer(fetchDelay),
	}
}

func (mq *InMemoryQueue) Put(ctx context.Context, record *models.Notification) error {
	mq.mx.Lock()
	defer mq.mx.Unlock()
	mq.storage.Add(record)
	return nil
}

func (mq *InMemoryQueue) Discard(ctx context.Context, user *models.User, chatID int) error {
	mq.mx.Lock()
	defer mq.mx.Unlock()
	mq.storage = mq.storage.Select(func(index int, value interface{}) bool {
		record := value.(*models.Notification)
		return !(record.ChatID == chatID && record.User.ID == user.ID)
	})
	return nil
}

func (mq *InMemoryQueue) GetNext() (*models.Notification, bool) {
	var result *models.Notification
	isStopped := mq.FetchLoop(func() bool {
		var isFetched bool
		result, isFetched = mq.tryGetNext()
		return isFetched
	})
	return result, isStopped
}

func (mq *InMemoryQueue) tryGetNext() (*models.Notification, bool) {
	mq.mx.Lock()
	defer mq.mx.Unlock()
	iterator := mq.storage.Iterator()
	if !iterator.First() {
		return nil, false
	}
	record := iterator.Value().(*models.Notification)
	if record.ReadyAt.After(time.Now()) {
		return nil, false
	}
	mq.storage.Remove(record)
	return record, true
}
