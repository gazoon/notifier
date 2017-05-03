package core

import (
	"notifier/config"
	"notifier/logging"
	"notifier/neo"
	"sync"

	"context"
	"notifier/telegram"

	log "github.com/Sirupsen/logrus"
	"notifier/tracing"
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

func prepareContext() context.Context {
	requestID := tracing.NewRequestID()
	ctx := tracing.NewContext(context.Background(), requestID)
	logger := logging.WithRequestID(requestID)
	ctx = logging.NewContext(ctx, logger)
	return ctx
}

func listenForMessages() {
	wg := sync.WaitGroup{}
	messages := telegram.MessagesChan
	for i := 0; i < config.WorkersNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range messages {
				ctx := prepareContext()
				dispatchMessage(ctx, msg)
			}
		}()
	}
	wg.Wait()
}

func RunServer(confPath string) {
	Initialization(confPath)
	log.Info("Successfully started")
	listenForMessages()
}
