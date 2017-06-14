package main

import (
	"context"
	"flag"
	"notifier/bot"
	"notifier/config"
	"notifier/core"
)

func main() {
	var confPath string
	config.FromCmdArgs(&confPath)
	flag.Parse()

	core.Initialization(confPath)

	db, err := core.CreateNeoStorageDBClient()
	if err != nil {
		panic(err)
	}
	params := map[string]interface{}{"method": bot.AllMentioningMethod}
	err = db.Exec(context.Background(), `match (u:User) set u.mentioning={method}`, params)
	if err != nil {
		panic(err)
	}
}
