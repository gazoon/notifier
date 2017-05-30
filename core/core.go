package core

import (
	"notifier/config"
	"notifier/logging"
	"notifier/messenger"
	"notifier/storage"
	"os"
	"os/signal"
	"syscall"

	"notifier/queue/messages"
	"notifier/queue/notifications"

	"github.com/pkg/errors"
	"reflect"
)

var (
	gLogger = logging.WithPackage("core")
)

func Initialization(confPath string) {
	config.Initialization(confPath)
	conf := config.GetInstance()
	logging.PatchStdLog(conf.Logging.DefaultLevel, conf.ServiceName, conf.ServerID)
	gLogger.Info("Environment has been initialized")
}

func CreateMongoMsgs() (*msgsqueue.MongoQueue, error) {
	conf := config.GetInstance()
	gLogger.Info("Initializing mongo messages queue")
	incomingMongoQueue, err := msgsqueue.NewMongoQueue(conf.MongoMessages.Database, conf.MongoMessages.User,
		conf.MongoMessages.Password, conf.MongoMessages.Host, conf.MongoMessages.Port, conf.MongoMessages.Timeout,
		conf.MongoMessages.PoolSize, conf.MongoMessages.FetchDelay)
	return incomingMongoQueue, errors.Wrap(err, "mongo messages queue")
}

func CreateMongoNotifications() (*notifqueue.MongoQueue, error) {
	conf := config.GetInstance()
	gLogger.Info("Initializing mongo notification queue")
	outgoingMongoQueue, err := notifqueue.NewMongoQueue(conf.MongoNotification.Database, conf.MongoNotification.User,
		conf.MongoNotification.Password, conf.MongoNotification.Host, conf.MongoNotification.Port,
		conf.MongoNotification.Timeout, conf.MongoNotification.PoolSize, conf.MongoNotification.FetchDelay)
	return outgoingMongoQueue, errors.Wrap(err, "mongo notification queue")
}

func CreateNeoStorage() (*storage.NeoStorage, error) {
	gLogger.Info("Initializing neo storage")
	conf := config.GetInstance()
	dataStorage, err := storage.NewNeoStorage(conf.Neo.Host, conf.Neo.Port, conf.Neo.User, conf.Neo.Password,
		conf.Neo.Timeout, conf.Neo.PoolSize)
	return dataStorage, errors.Wrap(err, "neo storage")
}

func CreateTelegramMessenger() (messenger.Messenger, error) {
	conf := config.GetInstance()
	gLogger.Info("Initializing messenger API")
	telegramMessenger, err := messenger.NewTelegram(conf.Telegram.APIToken, conf.Telegram.HttpTimeout)
	return telegramMessenger, errors.Wrap(err, "telegram messenger")
}

func WaitingForShutdown() {
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	gLogger.Infof("Received shutdown signal: %s", <-ch)
}

type Indexable interface {
	PrepareIndexes() error
}

func PrepareIndexes(databases ...Indexable) error {
	for _, db := range databases {
		indexerName := reflect.TypeOf(db)
		gLogger.Infof("Creating indexes for %s", indexerName)
		err := db.PrepareIndexes()
		if err != nil {
			return errors.Wrapf(err, "cannot prepare indexes, indexer: %s", indexerName)
		}
		gLogger.Infof("Indexes for %s created", indexerName)
	}
	return nil
}
