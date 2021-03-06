package notifqueue

import (
	"context"
	"notifier/models"
	"sync"

	"time"

	"github.com/emirpasic/gods/sets/treeset"
	"github.com/gazoon/bot_libs/logging"
	"github.com/gazoon/bot_libs/mongo"
	"github.com/gazoon/bot_libs/queue"
	"github.com/pkg/errors"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

var (
	gLogger = logging.WithPackage("notification_queue")
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

func NewMongoQueue(database, collection, user, password, host string, port, timeout, poolSize, retriesNum, retriesInterval,
	fetchDelay int) (*MongoQueue, error) {

	client, err := mongo.NewClient(database, collection, user, password, host, port, timeout, poolSize, retriesNum,
		retriesInterval)
	if err != nil {
		return nil, err
	}
	return &MongoQueue{client: client, BaseConsumer: queue.NewBaseConsumer(fetchDelay)}, nil
}

func (mq *MongoQueue) Put(ctx context.Context, record *models.Notification) error {
	err := mq.client.InsertRetry(ctx, record)
	if err == mongo.DuplicateKeyErr {
		logger := logging.FromContextAndBase(ctx, gLogger)
		logger.WithField("record", record).Warn("Notification duplication")
		return nil
	}
	return err
}

func (mq *MongoQueue) Discard(ctx context.Context, user *models.User, chatID int) error {
	_, err := mq.client.Remove(ctx, bson.M{"user_id": user.ID, "chat_id": chatID})
	return err
}

func (mq *MongoQueue) GetNext() (*models.Notification, bool) {
	var result *models.Notification
	isStopped := mq.FetchLoop(func() bool {
		var isFetched bool
		result, isFetched = mq.tryGetNext()
		return isFetched
	})
	return result, isStopped
}

func (mq *MongoQueue) tryGetNext() (*models.Notification, bool) {
	result := &models.Notification{}
	err := mq.client.FindAndModify(context.Background(),
		bson.M{"ready_at": bson.M{"$lt": time.Now().UTC()}},
		"ready_at",
		mgo.Change{Remove: true},
		result)
	if err != nil {
		if err != mgo.ErrNotFound {
			gLogger.Errorf("Cannot fetch documnt from mongo: %s", err)
		}
		return nil, false
	}
	return result, true
}

func (mq *MongoQueue) PrepareIndexes() error {
	var err error

	err = mq.client.CreateIndex(false, false, "ready_at")
	if err != nil {
		return errors.Wrap(err, "ready_at index")
	}

	err = mq.client.CreateIndex(true, false, "user_id", "chat_id", "message_id")
	if err != nil {
		return errors.Wrap(err, "user_id+chat_id index")
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
		return !(record.ChatID == chatID && record.UserID == user.ID)
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
	if record.ReadyAt.After(time.Now().UTC()) {
		return nil, false
	}
	mq.storage.Remove(record)
	return record, true
}
