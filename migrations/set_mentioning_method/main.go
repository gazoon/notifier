package main

import (
	"context"
	"flag"
	"notifier/config"
	"notifier/core"
	"notifier/libs/models"
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
	params := map[string]interface{}{"method": models.AllMentioningMethod}
	err = db.Exec(context.Background(), `match (u:User) set u.mentioning={method}`, params)
	if err != nil {
		panic(err)
	}
}
