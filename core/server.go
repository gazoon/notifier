package core

import (
	"notifier/config"
	"notifier/logging"
	"notifier/neo"
	"sync"

	"context"
	"notifier/telegram"

	log "github.com/Sirupsen/logrus"
)

const (
	addLabelCmd    = "addLabel"
	removeLabelCmd = "removeLabel"
	showLabelsCmd  = "showLabels"
)

func Initialization(confPath string) {
	config.Initialization(confPath)
	neo.Initialization()
	logging.Initialization()
	telegram.Initialization()
}

func RunServer(confPath string) {
	Initialization(confPath)
	wg := sync.WaitGroup{}
	messages := telegram.MessagesChan
	for i := 0; i < config.WorkersNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range messages {
				ctx := context.TODO()
				dispatchMessage(ctx, msg)
			}
		}()
	}
	log.Info("Successfully started")
	wg.Wait()
}
