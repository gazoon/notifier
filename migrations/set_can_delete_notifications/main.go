package main

import (
	"flag"
	"notifier/config"
	"notifier/core"
	"context"
	"notifier/models"
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
	params := map[string]interface{}{"can_delete":models.DefaultDeleteNotificationsFlag}
	err = db.Exec(context.Background(), `match (u:User) set u.delete_notifications={can_delete}`, params)
	if err != nil {
		panic(err)
	}
}

