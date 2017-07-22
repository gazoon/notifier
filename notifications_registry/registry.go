package notifregistry

import (
	"context"
	"time"

	"github.com/gazoon/bot_libs/logging"
	"github.com/gazoon/bot_libs/mongo"
	"github.com/pkg/errors"
	"gopkg.in/mgo.v2/bson"
)

const (
	maxMessageToProcess = 100
)

var (
	gLogger = logging.WithPackage("notifications_registry")
)

type SentNotification struct {
	UserID     int       `bson:"user_id"`
	MessageID  int       `bson:"message_id"`
	FromChatID int       `bson:"from_chat_id"`
	SentAt     time.Time `bson:"sent_at"`
}

func (sn SentNotification) String() string {
	return logging.ObjToString(&sn)
}

type Saver interface {
	Save(ctx context.Context, msg *SentNotification) error
}

type ReadDeleter interface {
	Get(ctx context.Context, userID, fromChatID int) ([]*SentNotification, error)
	Delete(ctx context.Context, msg *SentNotification) error
	DeleteAllForUser(ctx context.Context, userID int) error
}

type MongoRegistry struct {
	db *mongo.Client
}

func NewMongoRegistry(database, collection, user, password, host string, port, timeout, poolSize, retriesNum, retriesInterval int) (
	*MongoRegistry, error) {

	client, err := mongo.NewClient(database, collection, user, password, host, port, timeout, poolSize, retriesNum,
		retriesInterval)
	if err != nil {
		return nil, err
	}
	return &MongoRegistry{db: client}, nil
}

func (r *MongoRegistry) Save(ctx context.Context, msg *SentNotification) error {
	err := r.db.InsertRetry(ctx, msg)
	if err == mongo.DuplicateKeyErr {
		logger := logging.FromContextAndBase(ctx, gLogger)
		logger.WithField("sent_msg", msg).Warn("This msg already exists in the db")
		return nil
	}
	return err
}

func (r *MongoRegistry) Delete(ctx context.Context, msg *SentNotification) error {
	_, err := r.db.Remove(ctx, bson.M{"user_id": msg.UserID, "message_id": msg.MessageID})
	return err
}

func (r *MongoRegistry) DeleteAllForUser(ctx context.Context, userID int) error {
	_, err := r.db.Remove(ctx, bson.M{"user_id": userID})
	return err
}

func (r *MongoRegistry) Get(ctx context.Context, userID, fromChatID int) ([]*SentNotification, error) {
	var messages []*SentNotification
	err := r.db.Find(ctx, bson.M{"user_id": userID, "from_chat_id": fromChatID}, "sent_at", maxMessageToProcess, &messages)
	return messages, err
}

func (r *MongoRegistry) PrepareIndexes() error {
	var err error
	err = r.db.CreateIndex(true, false, "user_id", "message_id")
	if err != nil {
		return errors.Wrap(err, "user_id+message_id index")
	}
	err = r.db.CreateIndex(false, false, "user_id", "from_chat_id", "sent_at")
	if err != nil {
		return errors.Wrap(err, "user_id+from_chat_id+sent_at index")
	}
	return nil
}
