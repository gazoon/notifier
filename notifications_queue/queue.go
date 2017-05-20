package notifqueue

import (
	"context"
	"notifier/models"
	"strconv"
	"sync"

	"time"

	"notifier/logging"
	"sync/atomic"

	"github.com/emirpasic/gods/sets/treeset"
	"github.com/pkg/errors"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"notifier/mongo"
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

type InMemoryQueue struct {
	storageByTime     *treeset.Set
	storageByIdentity map[string][]*models.Notification
	readClosed        int32
	mx                sync.Mutex
}

func NewInMemory() *InMemoryQueue {
	comparator := func(a, b interface{}) int {
		aRecord := a.(*models.Notification)
		bRecord := b.(*models.Notification)
		if aRecord.ReadyAt.Before(bRecord.ReadyAt) {
			return -1
		}
		if aRecord.ReadyAt.After(bRecord.ReadyAt) {
			return 1
		}
		if aRecord.ID == bRecord.ID {
			return 0
		}
		return -1
	}
	return &InMemoryQueue{
		storageByTime:     treeset.NewWith(comparator),
		storageByIdentity: map[string][]*models.Notification{},
	}
}

func (mq *InMemoryQueue) buildIdentityKey(userID, chatID int) string {
	key := strconv.Itoa(userID) + strconv.Itoa(chatID)
	return key
}

func (mq *InMemoryQueue) Put(ctx context.Context, record *models.Notification) error {
	userIdentity := mq.buildIdentityKey(record.User.ID, record.ChatID)
	mq.mx.Lock()
	defer mq.mx.Unlock()
	mq.storageByIdentity[userIdentity] = append(mq.storageByIdentity[userIdentity], record)
	mq.storageByTime.Add(record)
	return nil
}

func (mq *InMemoryQueue) Discard(ctx context.Context, user *models.User, chatID int) error {
	userIdentity := mq.buildIdentityKey(user.ID, chatID)
	mq.mx.Lock()
	defer mq.mx.Unlock()
	userRecords := mq.storageByIdentity[userIdentity]
	if len(userRecords) != 0 {
		for _, record := range userRecords {
			mq.storageByTime.Remove(record)
		}
		delete(mq.storageByIdentity, userIdentity)
	}
	return nil
}

func (mq *InMemoryQueue) GetNext() (*models.Notification, bool) {
	for {

		if atomic.LoadInt32(&mq.readClosed) == 1 {
			return nil, false
		}
		mq.mx.Lock()
		record, ok := mq.tryGetFirstByTime()
		if !ok {
			mq.mx.Unlock()
			const fetch_delay = 10
			time.Sleep(time.Duration(fetch_delay) * time.Millisecond)
			continue
		}
		mq.storageByTime.Remove(record)
		mq.removeFromIdentityStorage(record)
		mq.mx.Unlock()
		return record, true
	}
}

func (mq *InMemoryQueue) removeFromIdentityStorage(recordToDel *models.Notification) {
	userIdentity := mq.buildIdentityKey(recordToDel.User.ID, recordToDel.ChatID)
	records := mq.storageByIdentity[userIdentity]
	for i, record := range records {
		if record.ID == recordToDel.ID {
			lastIndex := len(records) - 1
			records[i] = records[lastIndex]
			records[lastIndex] = nil
			mq.storageByIdentity[userIdentity] = records[:lastIndex]
			return
		}
	}
}

func (mq *InMemoryQueue) tryGetFirstByTime() (*models.Notification, bool) {
	iterator := mq.storageByTime.Iterator()
	if !iterator.First() {
		return nil, false
	}
	record := iterator.Value().(*models.Notification)
	if record.ReadyAt.After(time.Now()) {
		return nil, false
	}
	return record, true
}

func (mq *InMemoryQueue) StopGivingMsgs() {
	atomic.StoreInt32(&mq.readClosed, 1)
}

type MongoQueue struct {
	client     *mongo.Client
	readClosed int32
}

func NewMongoQueue(database, user, password, host string, port, timeout, poolSize int) (*MongoQueue, error) {
	client, err := mongo.NewClient(database, mongoCollection, user, password, host, port, timeout, poolSize)
	if err != nil {
		return nil, errors.Wrap(err, "mongo initialization failed")
	}
	return &MongoQueue{client: client}, nil
}

func (mq *MongoQueue) Put(ctx context.Context, record *models.Notification) error {
	err := mq.client.Insert(ctx, record)
	return err
}

func (mq *MongoQueue) Discard(ctx context.Context, user *models.User, chatID int) error {
	err := mq.client.Remove(ctx, bson.M{"user.id": user.ID, "chat_id": chatID})
	return err
}

func (mq *MongoQueue) StopGivingMsgs() {
	atomic.StoreInt32(&mq.readClosed, 1)
}

func (mq *MongoQueue) GetNext() (*models.Notification, bool) {
	for {
		if atomic.LoadInt32(&mq.readClosed) == 1 {
			return nil, false
		}
		model := &models.Notification{}
		err := mq.client.FindAndModify(context.Background(),
			bson.M{"ready_at": bson.M{"$lt": time.Now()}},
			"ready_at",
			mgo.Change{Remove: true},
			model)
		if err != nil {
			if err != mgo.ErrNotFound {
				gLogger.Errorf("Cannot fetch documnt from mongo: %s", err)
			}
			const fetch_delay = 10
			time.Sleep(time.Duration(fetch_delay) * time.Millisecond)
			continue
		}
		return model, true
	}
}
