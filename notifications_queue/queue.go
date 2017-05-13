package notifqueue

import (
	"context"
	"notifier/models"
	"strconv"
	"sync"

	"time"

	"github.com/emirpasic/gods/sets/treeset"
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
	canGiveMsgs       bool
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
		canGiveMsgs:       true,
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
		if !mq.canGiveMsgs {
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
	mq.canGiveMsgs = false
}
