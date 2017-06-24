package main

import (
	"flag"
	"notifier/bot"
	"notifier/config"
	"notifier/core"
	"notifier/gateway"
	"notifier/libs/logging"
	"notifier/sender"

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
	botService := bot.New(incomingQueue, outgoingQueue, mongoNotificationsRegistry, telegramMessenger, dataStorage, googleRecognizer)
	pollerService := gateway.NewTelegramPoller(incomingQueue, conf.Telegram.APIToken, conf.Telegram.BotName,
		conf.TelegramPolling.PollTimeout, conf.TelegramPolling.RetryDelay)
	senderService := sender.New(outgoingQueue, mongoNotificationsRegistry, telegramMessenger, dataStorage)
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
	logging.StartLevelToggle(conf.Logging.TogglePath, conf.Logging.TogglePort)
	gLogger.Info("Server successfully started")
	core.WaitingForShutdown()
}
