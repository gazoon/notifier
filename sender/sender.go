package sender

import (
	"context"
	"notifier/config"
	"notifier/libs/logging"
	"notifier/libs/messenger"
	"notifier/libs/models"
	"notifier/libs/notifications_registry"
	"notifier/libs/queue/notifications"
	"sync"
	"time"
)

var (
	gLogger = logging.WithPackage("sender")
)

type Sender struct {
	notificationQueue notifqueue.Consumer
	messenger         messenger.Messenger
	registry          notifregistry.Saver
	wg                sync.WaitGroup
}

func prepareContext(requestID string) context.Context {
	ctx := logging.NewContext(context.Background(), logging.WithRequestID(requestID))
	return ctx
}

func New(notificationQueue notifqueue.Consumer, registry notifregistry.Saver, messenger messenger.Messenger) *Sender {
	return &Sender{notificationQueue: notificationQueue, registry: registry, messenger: messenger}
}

func (s *Sender) Start() {
	conf := config.GetInstance()
	gLogger.WithField("workers_num", conf.SenderWorkerNum).Info("Listening for notifications")
	for i := 0; i < conf.SenderWorkerNum; i++ {
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
	sentMessageIDs := s.sendNotification(ctx, notification)
	s.saveToRegistry(ctx, notification, sentMessageIDs)
}

func (s *Sender) saveToRegistry(ctx context.Context, notification *models.Notification, messageIDs []int) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	for _, messageID := range messageIDs {
		message := &notifregistry.SentNotification{
			UserID:     notification.User.ID,
			FromChatID: notification.ChatID,
			MessageID:  messageID,
			SentAt:     time.Now(),
		}
		logger = logger.WithField("sent_notification", message)
		logger.Info("Save message sent to the user to the registry")
		err := s.registry.Save(ctx, message)
		if err != nil {
			logger.Errorf("Registry save failed: %s", err)
		}
	}
}

func (s *Sender) sendNotification(ctx context.Context, notification *models.Notification) []int {
	logger := logging.FromContextAndBase(ctx, gLogger)
	var sentMessageIDs []int
	logger.WithField("user", notification.User).Info("Sending notification to the user")
	textMsgID, err := s.messenger.SendText(ctx, notification.User.PMID, notification.Text)
	if err != nil {
		logger.Errorf("Cannot sent notification text to the user: %s", err)
		return sentMessageIDs
	}
	sentMessageIDs = append(sentMessageIDs, textMsgID)
	forwardMsgID, err := s.messenger.SendForward(ctx, notification.User.PMID, notification.ChatID, notification.MessageID)
	if err != nil {
		logger.Errorf("Cannot sent notification forward to the user: %s", err)
		return sentMessageIDs
	}
	sentMessageIDs = append(sentMessageIDs, forwardMsgID)
	return sentMessageIDs
}

func (s *Sender) Stop() {
	gLogger.Info("Closing notification queue for reading")
	s.notificationQueue.StopGivingMsgs()
	gLogger.Info("Waiting until all workers will process the remaining notifications")
	s.wg.Wait()
	gLogger.Info("All workers've been stopped")
}
