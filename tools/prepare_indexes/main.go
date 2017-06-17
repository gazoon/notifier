package main

import (
	"flag"
	"notifier/config"
	"notifier/core"
)

func main() {
	var confPath string
	config.FromCmdArgs(&confPath)
	flag.Parse()

	core.Initialization(confPath)

	incomingMongoQueue, err := core.CreateMongoMsgs()
	if err != nil {
		panic(err)
	}
	outgoingMongoQueue, err := core.CreateMongoNotifications()
	if err != nil {
		panic(err)
	}
	mongoRegistry, err := core.CreateMongoNotificationsRegistry()
	if err != nil {
		panic(err)
	}
	neoStorage, err := core.CreateNeoStorage()
	if err != nil {
		panic(err)
	}

	err = core.PrepareIndexes(incomingMongoQueue, mongoRegistry, outgoingMongoQueue, neoStorage)
	if err != nil {
		panic(err)
	}
}
