package core

import (
	"notifier/config"
	"notifier/libs/messenger"
	"notifier/storage"
	"os"
	"os/signal"
	"syscall"

	"notifier/libs/queue/messages"
	"notifier/libs/queue/notifications"

	"github.com/pkg/errors"
	"notifier/libs/logging"
	"reflect"
	"notifier/libs/speech"
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
	conf := config.GetInstance().MongoMessages
	gLogger.Info("Initializing mongo messages queue")
	incomingMongoQueue, err := msgsqueue.NewMongoQueue(conf.Database, conf.User, conf.Password, conf.Host, conf.Port,
		conf.Timeout, conf.PoolSize, conf.RetriesNum, conf.RetriesInterval, conf.FetchDelay)
	return incomingMongoQueue, errors.Wrap(err, "mongo messages queue")
}

func CreateMongoNotifications() (*notifqueue.MongoQueue, error) {
	conf := config.GetInstance().MongoNotification
	gLogger.Info("Initializing mongo notification queue")
	outgoingMongoQueue, err := notifqueue.NewMongoQueue(conf.Database, conf.User, conf.Password, conf.Host, conf.Port,
		conf.Timeout, conf.PoolSize, conf.RetriesNum, conf.RetriesInterval, conf.FetchDelay)
	return outgoingMongoQueue, errors.Wrap(err, "mongo notification queue")
}

func CreateNeoStorage() (*storage.NeoStorage, error) {
	gLogger.Info("Initializing neo storage")
	conf := config.GetInstance()
	dataStorage, err := storage.NewNeoStorage(conf.Neo.Host, conf.Neo.Port, conf.Neo.User, conf.Neo.Password,
		conf.Neo.Timeout, conf.Neo.PoolSize, conf.Neo.RetriesNum, conf.Neo.RetriesInterval)
	return dataStorage, errors.Wrap(err, "neo storage")
}

func CreateGoogleRecognizer() *speech.GoogleRecognizer {
	gLogger.Info("Initializing google recognizer api")
	conf := config.GetInstance()
	recognizer := speech.NewGoogleRecognizer(conf.GoogleAPI.APIKey, conf.GoogleAPI.HttpTimeout)
	return recognizer
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
