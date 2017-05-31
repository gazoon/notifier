package sender

import (
	"context"
	"notifier/config"
	"notifier/libs/logging"
	"notifier/libs/messenger"
	"notifier/libs/models"
	"notifier/libs/queue/notifications"
	"sync"
)

var (
	gLogger = logging.WithPackage("sender")
)

type Sender struct {
	notificationQueue notifqueue.Consumer
	messenger         messenger.Messenger
	wg                sync.WaitGroup
}

func prepareContext(requestID string) context.Context {
	ctx := logging.NewContext(context.Background(), logging.WithRequestID(requestID))
	return ctx
}

func New(notificationQueue notifqueue.Consumer, messenger messenger.Messenger) *Sender {
	return &Sender{notificationQueue: notificationQueue, messenger: messenger}
}

func (s *Sender) Start() {
	conf := config.GetInstance()
	gLogger.WithField("workers_num", conf.SenderWorkerNum).Info("Listening for notifications")
	for i := 0; i < conf.BotWorkersNum; i++ {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			for {
				gLogger.Info("Fetching new notification from queue")
				notification, ok := s.notificationQueue.GetNext()
				if !ok {
					return
				}
				s.onNotification(notification)
			}
		}()
	}
}

func (s *Sender) onNotification(notification *models.Notification) {
	ctx := prepareContext(notification.RequestID)
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("notification", notification).Info("Notification received from outgoing queue")
	s.sendNotification(ctx, notification)
}

func (s *Sender) Stop() {
	gLogger.Info("Closing notification queue for reading")
	s.notificationQueue.StopGivingMsgs()
	gLogger.Info("Waiting until all workers will process the remaining notifications")
	s.wg.Wait()
	gLogger.Info("All workers've been stopped")
}

func (s *Sender) sendNotification(ctx context.Context, notification *models.Notification) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("user", notification.User).Info("Sending notification to the user")
	err := s.messenger.SendForwardWithText(ctx, notification.User.PMID, notification.ChatID, notification.MessageID,
		notification.Text)
	if err != nil {
		logger.Errorf("Cannot sent notification to the user: %s", err)
	}
}
