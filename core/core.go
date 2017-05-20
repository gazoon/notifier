package core

import (
	"notifier/bot"
	"notifier/config"
	"notifier/gateway"
	"notifier/logging"
	"notifier/messenger"
	"notifier/storage"
	"os"
	"os/signal"
	"syscall"

	"notifier/queue/messages"
	"notifier/sender"

	"notifier/queue/notifications"

	"github.com/pkg/errors"
)

var (
	gLogger = logging.WithPackage("core")
)

func Initialization(confPath string) {
	config.Initialization(confPath)
	conf := config.GetInstance()
	logging.PatchStdLog(conf.LogLevel, conf.ServiceName, conf.ServerID)
	gLogger.Info("Environment has been initialized")
}

func Run(confPath string) {
	Initialization(confPath)
	conf := config.GetInstance()
	gLogger.Info("Initializing mongo messages queue")
	incomingMongoQueue, err := msgsqueue.NewMongoQueue(conf.MongoMessages.Database, conf.MongoMessages.User,
		conf.MongoMessages.Password, conf.MongoMessages.Host, conf.MongoMessages.Port, conf.MongoMessages.Timeout,
		conf.MongoMessages.PoolSize, conf.MongoMessages.FetchDelay)
	if err != nil {
		panic(errors.Wrap(err, "mongo messages queue"))
	}
	gLogger.Info("Initializing mongo notification queue")
	outgoingMongoQueue, err := notifqueue.NewMongoQueue(conf.MongoNotification.Database, conf.MongoNotification.User,
		conf.MongoNotification.Password, conf.MongoNotification.Host, conf.MongoNotification.Port,
		conf.MongoNotification.Timeout, conf.MongoNotification.PoolSize, conf.MongoNotification.FetchDelay)
	if err != nil {
		panic(errors.Wrap(err, "mongo notification queue"))
	}
	gLogger.Info("Initializing messenger API")
	telegramMessenger, err := messenger.NewTelegram(conf.Telegram.APIToken, conf.Telegram.HttpTimeout)
	if err != nil {
		panic(errors.Wrap(err, "cannot initialize telegram messenger API"))
	}
	gLogger.Info("Initializing neo storage")
	dataStorage, err := storage.NewNeoStorage()
	if err != nil {
		panic(errors.Wrap(err, "neo storage"))
	}
	botService := bot.New(incomingMongoQueue, outgoingMongoQueue, telegramMessenger, dataStorage)
	pollerService := gateway.NewTelegramPoller(incomingMongoQueue)
	senderService := sender.New(outgoingMongoQueue, telegramMessenger)
	gLogger.Info("Starting bot service")
	botService.Start()
	defer botService.Stop()
	gLogger.Info("Starting sender service")
	senderService.Start()
	defer senderService.Stop()
	gLogger.Info("Starting telegram poller service")
	err = pollerService.Start()
	if err != nil {
		panic(errors.Wrap(err, "cannot start poller"))
	}
	gLogger.Info("Server successfully started")
	waitingForShutdown()
}

func waitingForShutdown() {
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	gLogger.Infof("Received shutdown signal: %s", <-ch)
}
