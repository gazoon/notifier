package main

import (
	"flag"
	"notifier/bot"
	"notifier/core"
	"notifier/gateway"
	"notifier/logging"
	"notifier/sender"

	"notifier/config"

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
	botService := bot.New(incomingQueue, outgoingQueue, telegramMessenger, dataStorage)
	pollerService := gateway.NewTelegramPoller(incomingQueue, conf.Telegram.BotName)
	senderService := sender.New(outgoingQueue, telegramMessenger)
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
	core.WaitingForShutdown()
}
