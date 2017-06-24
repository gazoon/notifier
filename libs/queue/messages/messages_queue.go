package msgsqueue

import (
	"context"
	"notifier/libs/logging"
	"sync"
	"time"

	"notifier/libs/mongo"

	"github.com/emirpasic/gods/sets/treeset"
	"notifier/libs/queue"

	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	MAX_PROCESSING_TIME = 20 * time.Second
	mongoCollection     = "messages"
)

var (
	gLogger         = logging.WithPackage("incoming_queue")
	DuplicateMsgErr = errors.New("message is already in the queue")
)

type Producer interface {
	Put(ctx context.Context, msg *Message) error
}

type Consumer interface {
	GetNext() (*Message, string, bool)
	FinishProcessing(ctx context.Context, processingID string)
	StopGivingMsgs()
}

type MongoQueue struct {
	client *mongo.Client
	*queue.BaseConsumer
}

func NewMongoQueue(database, user, password, host string, port, timeout, poolSize, retriesNum, retriesInterval,
	fetchDelay int) (*MongoQueue, error) {

	client, err := mongo.NewClient(database, mongoCollection, user, password, host, port, timeout, poolSize, retriesNum,
		retriesInterval)
	if err != nil {
		return nil, err
	}
	return &MongoQueue{client: client, BaseConsumer: queue.NewBaseConsumer(fetchDelay)}, nil
}

func (mq *MongoQueue) Put(ctx context.Context, msg *Message) error {
	err := mq.client.UpsertRetry(ctx,
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

func (mq *MongoQueue) GetNext() (*Message, string, bool) {
	var result *Message
	var processingID string
	isStopped := mq.FetchLoop(func() bool {
		var isFetched bool
		result, processingID, isFetched = mq.tryGetNext()
		return isFetched
	})
	return result, processingID, isStopped
}

func (mq *MongoQueue) tryGetNext() (*Message, string, bool) {
	var doc struct {
		ChatID int        `bson:"chat_id"`
		Msgs   []*Message `bson:"msgs"`
	}
	currentTime := time.Now()
	processingID := newProcessingID()
	err := mq.client.FindAndModify(context.Background(),
		bson.M{
			"$or": []bson.M{
				{"processed_at": bson.M{"$exists": false}},
				{"processed_at": bson.M{"$lt": currentTime.Add(-MAX_PROCESSING_TIME)}},
			}},
		"msgs.0.created_at",
		mgo.Change{Update: bson.M{
			"$set": bson.M{"processed_at": currentTime, "processing_id": processingID},
			"$pop": bson.M{"msgs": -1},
		}},
		&doc)
	if err != nil {
		if err != mgo.ErrNotFound {
			gLogger.Errorf("Cannot fetch document from mongo: %s", err)
		}
		return nil, "", false
	}
	if len(doc.Msgs) == 0 {
		logger := gLogger.WithField("chat_id", doc.ChatID)
		ctx := logging.NewContextBackground(logger)
		logger.Warn("Got document without messages, finish processing")
		mq.FinishProcessing(ctx, processingID)
		return nil, "", false
	}
	return doc.Msgs[0], processingID, true
}

func (mq *MongoQueue) FinishProcessing(ctx context.Context, processingID string) {
	logger := logging.FromContextAndBase(ctx, gLogger).WithField("processing_id", processingID)
	logger.Info("Remove the document in case it doesn't contain messages")
	docNum, err := mq.client.Remove(ctx, bson.M{"msgs": []interface{}{}, "processing_id": processingID})
	if err != nil {
		logger := logging.FromContextAndBase(ctx, gLogger)
		logger.Errorf("Cannot remove empty chat queue: %s", err)
		docNum = 0
	}
	if docNum > 0 {
		return
	}
	logger.Info("The document hasn't been removed, reset processing info")
	err = mq.client.UpdateRetry(ctx,
		bson.M{"processing_id": processingID},
		bson.M{"$unset": bson.M{"processing_id": "", "processed_at": ""}},
	)
	if err == mgo.ErrNotFound {
		logger.Warn("Message document with that processing_id no longer exists")
		return
	}
	if err != nil {
		logger.Errorf("Cannot reset processing: %s", err)
	}
}

func (mq *MongoQueue) PrepareIndexes() error {
	var err error

	err = mq.client.CreateIndex(false, false, "msgs.0.created_at")
	if err != nil {
		return errors.Wrap(err, "created_at index")
	}

	err = mq.client.CreateIndex(true, true, "processing_id")
	if err != nil {
		return errors.Wrap(err, "processing_id unique sparse index")
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

type chatMessages struct {
	chatID       int
	msgs         []*Message
	processedAt  *time.Time
	processingID *string
}

type InMemoryQueue struct {
	storage *treeset.Set
	mx      sync.Mutex
	*queue.BaseConsumer
}

func NewInMemory() *InMemoryQueue {
	comparator := func(a, b interface{}) int {
		aRecord := a.(*chatMessages)
		bRecord := b.(*chatMessages)
		if aRecord.chatID == bRecord.chatID {
			return 0
		}
		if len(bRecord.msgs) == 0 {
			return -1
		}
		if len(aRecord.msgs) == 0 {
			return 1
		}
		if aRecord.msgs[0].CreatedAt.Before(bRecord.msgs[0].CreatedAt) {
			return -1
		}
		if aRecord.msgs[0].CreatedAt.After(bRecord.msgs[0].CreatedAt) {
			return 1
		}
		return -1
	}
	const fetchDelay = 50
	return &InMemoryQueue{
		BaseConsumer: queue.NewBaseConsumer(fetchDelay),
		storage:      treeset.NewWith(comparator),
	}
}

func (mq *InMemoryQueue) GetNext() (*Message, string, bool) {
	var result *Message
	var processingID string
	isStopped := mq.FetchLoop(func() bool {
		var isFetched bool
		result, processingID, isFetched = mq.tryGetNext()
		return isFetched
	})
	return result, processingID, isStopped
}

func (mq *InMemoryQueue) tryGetNext() (*Message, string, bool) {
	mq.mx.Lock()
	defer mq.mx.Unlock()
	currentTime := time.Now()
	_, result := mq.storage.Find(func(index int, value interface{}) bool {
		chatQueue := value.(*chatMessages)
		return chatQueue.processedAt == nil || chatQueue.processedAt.Before(currentTime.Add(-MAX_PROCESSING_TIME))
	})
	if result == nil {
		return nil, "", false
	}
	chatQueue := result.(*chatMessages)
	mq.storage.Remove(chatQueue)
	if len(chatQueue.msgs) == 0 {
		gLogger.WithField("chat_id", chatQueue.chatID).Info("Got chatQueue without messages, remove from storages")
		return nil, "", false
	}
	processingID := newProcessingID()
	chatQueue.processingID = &processingID
	chatQueue.processedAt = &currentTime
	msg := chatQueue.msgs[0]
	chatQueue.msgs = chatQueue.msgs[1:]
	mq.storage.Add(chatQueue)
	return msg, processingID, true
}

func (mq *InMemoryQueue) Put(ctx context.Context, msg *Message) error {
	mq.mx.Lock()
	defer mq.mx.Unlock()
	_, result := mq.storage.Find(func(index int, value interface{}) bool {
		chatQueue := value.(*chatMessages)
		return chatQueue.chatID == msg.Chat.ID
	})
	var chatQueue *chatMessages
	if result != nil {
		chatQueue = result.(*chatMessages)
		for _, queuedMsg := range chatQueue.msgs {
			if queuedMsg.ID == msg.ID {
				return DuplicateMsgErr
			}
		}
	} else {
		chatQueue = &chatMessages{chatID: msg.Chat.ID}
	}
	isFirstMsg := len(chatQueue.msgs) == 0
	if isFirstMsg {
		mq.storage.Remove(chatQueue)
	}
	chatQueue.msgs = append(chatQueue.msgs, msg)
	if isFirstMsg {
		mq.storage.Add(chatQueue)
	}
	return nil
}

func (mq *InMemoryQueue) FinishProcessing(ctx context.Context, processingID string) {
	logger := logging.FromContextAndBase(ctx, gLogger).WithField("processing_id", processingID)
	mq.mx.Lock()
	defer mq.mx.Unlock()
	_, result := mq.storage.Find(func(index int, value interface{}) bool {
		chatQueue := value.(*chatMessages)
		return chatQueue.processingID != nil && *chatQueue.processingID == processingID
	})
	if result == nil {
		logger.Warn("No chatQueue found for that processing_id")
		return
	}
	chatQueue := result.(*chatMessages)
	if len(chatQueue.msgs) == 0 {
		logger.Info("chatQueue doesn't have any messages, removing from storages")
		mq.storage.Remove(chatQueue)
	} else {
		logger.Info("chatQueue contains messages, set processingID and processingAt to nil")
		chatQueue.processingID = nil
		chatQueue.processedAt = nil
	}
	return
}
