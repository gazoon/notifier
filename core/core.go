package core

import (
	"notifier/bot"
	"notifier/config"
	"notifier/gateway"
	"notifier/logging"
	"notifier/messenger"
	"notifier/neo"
	"notifier/notifications_queue"
	"notifier/storage"
	"os"
	"os/signal"
	"syscall"

	"github.com/pkg/errors"
	"notifier/messages_queue"
	"notifier/sender"
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
	incomingQueue, err := msgsqueue.NewMongoQueue(conf.MongoMessages.Database, conf.MongoMessages.User,
		conf.MongoMessages.Password, conf.MongoMessages.Host, conf.MongoMessages.Port, conf.MongoMessages.Timeout,
		conf.MongoMessages.PoolSize)
	if err != nil {
		panic(errors.Wrap(err, "mongo messages queue"))
	}
	//outMemoryQueue := notifqueue.NewInMemory()
	gLogger.Info("Initializing mongo notification queue")
	outMongoQueue, err := notifqueue.NewMongoQueue(conf.MongoNotification.Database, conf.MongoNotification.User,
		conf.MongoNotification.Password, conf.MongoNotification.Host, conf.MongoNotification.Port,
		conf.MongoNotification.Timeout, conf.MongoNotification.PoolSize)
	if err != nil {
		panic(errors.Wrap(err, "mongo notification queue"))
	}
	gLogger.Info("Initializing neo client")
	neoDB, err := neo.NewClient(conf.Neo.Host, conf.Neo.Port, conf.Neo.User, conf.Neo.Password, conf.Neo.Timeout,
		conf.Neo.PoolSize)
	if err != nil {
		panic(errors.Wrap(err, "cannot create neo client"))
	}
	gLogger.Info("Initializing messenger API")
	telegramMessenger, err := messenger.NewTelegram(conf.Telegram.APIToken, conf.Telegram.HttpTimeout)
	if err != nil {
		panic(errors.Wrap(err, "cannot initialize telegram messenger API"))
	}
	dataStorage := storage.NewNeoStorage(neoDB)
	botService := bot.New(incomingQueue, outMongoQueue, neoDB, telegramMessenger, dataStorage)
	pollerService := gateway.NewTelegramPoller(incomingQueue)
	senderService := sender.New(outMongoQueue, telegramMessenger)
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
