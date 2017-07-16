package main

import (
	"flag"
	"github.com/gazoon/bot_libs/logging"
	"notifier/bot"
	"notifier/config"
	"notifier/core"
	"notifier/sender"

	"github.com/gazoon/bot_libs/gateway"
	"github.com/gazoon/bot_libs/queue/messages"
	"github.com/gazoon/bot_libs/utils"
	"github.com/pkg/errors"
)

var (
	gLogger = logging.WithPackage("run")
)

func main() {
	var confPath string
	config.FromCmdArgs(&confPath)
	flag.Parse()

	core.Initialization(confPath)
	conf := config.GetInstance()
	incomingQueue, err := core.CreateMongoMsgs()
	if err != nil {
		panic(err)
	}
	//incomingQueue := msgsqueue.NewInMemory()
	//outgoingQueue := notifqueue.NewInMemory()
	outgoingQueue, err := core.CreateMongoNotifications()
	if err != nil {
		panic(err)
	}
	telegramMessenger, err := core.CreateTelegramMessenger()
	if err != nil {
		panic(err)
	}
	dataStorage, err := core.CreateNeoStorage()
	if err != nil {
		panic(err)
	}
	googleRecognizer := core.CreateGoogleRecognizer()
	mongoNotificationsRegistry, err := core.CreateMongoNotificationsRegistry()
	if err != nil {
		panic(err)
	}
	notifierBot := bot.New(outgoingQueue, mongoNotificationsRegistry, telegramMessenger, dataStorage, googleRecognizer)
	readerService := msgsqueue.NewReader(incomingQueue, conf.MongoMessages.WorkersNum, notifierBot.DispatchMessage)
	pollerService := gateway.NewTelegramPoller(incomingQueue, conf.Telegram.APIToken, conf.Telegram.BotName,
		conf.TelegramPolling.PollTimeout, conf.TelegramPolling.RetryDelay)
	senderService := sender.New(outgoingQueue, conf.MongoNotification.WorkersNum, mongoNotificationsRegistry,
		telegramMessenger, dataStorage)
	gLogger.Info("Starting bot service")
	readerService.Start()
	defer readerService.Stop()
	gLogger.Info("Starting sender service")
	senderService.Start()
	defer senderService.Stop()
	gLogger.Info("Starting telegram poller service")
	err = pollerService.Start()
	if err != nil {
		panic(errors.Wrap(err, "cannot start poller"))
	}
	logging.StartLevelToggle(conf.Logging.TogglePath, conf.Logging.TogglePort)
	gLogger.Info("Server successfully started")
	utils.WaitingForShutdown()
}
