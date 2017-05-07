package core

import (
	"notifier/config"
	"notifier/logging"
	"notifier/neo"
	"sync"

	"context"
	"fmt"
	"notifier/incoming"
	"notifier/sender"
	"notifier/tracing"

	"notifier/models"
	"notifier/polling"
)

const (
	addLabelCmd    = "addLabel"
	removeLabelCmd = "removeLabel"
	showLabelsCmd  = "showLabels"
)

var (
	CommandsText = fmt.Sprintf(`Hi! I can do following for you:\n%s - Add new label for further notifications.\n`+
		`%s - Delete label with provided name.\n%s - Show all your labels.`, addLabelCmd, removeLabelCmd, showLabelsCmd)
)

func Initialization(confPath string) {
	config.Initialization(confPath)
	logging.Initialization()
	neo.Initialization()
	incomming.Initialization()
	sender.Initialization()
}

func prepareContext(msg *models.Message) context.Context {
	requestID := msg.RequestID
	ctx := tracing.NewContext(context.Background(), requestID)
	logger := logging.WithRequestID(requestID)
	ctx = logging.NewContext(ctx, logger)
	return ctx
}

func listenForMessages() {
	wg := sync.WaitGroup{}
	conf := config.GetInstance()
	queue := incomming.GetQueue()
	for i := 0; i < conf.WorkersNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				queueMsg := queue.GetNext()
				msg := queueMsg.Payload()
				ctx := prepareContext(msg)
				dispatchMessage(ctx, msg)
				queueMsg.Ack()
			}
		}()
	}
	wg.Wait()
}

func RunServer(confPath string) {
	Initialization(confPath)
	gLogger.Info("Successfully started")
	poller:=polling.TelegramPoller{}
	poller.Start()
	listenForMessages()
}
