package sender

import (
	"context"
	"notifier/models"
	"notifier/notifications"
	"notifier/notifications_registry"
	"notifier/storage"
	"sync"
	"time"

	"github.com/gazoon/bot_libs/logging"
	"github.com/gazoon/bot_libs/messenger"
)

var (
	gLogger = logging.WithPackage("sender")
)

type Sender struct {
	notificationQueue notifqueue.Consumer
	messenger         messenger.Messenger
	storage           storage.Storage
	registry          notifregistry.Saver
	workersNum        int
	wg                sync.WaitGroup
}

func prepareContext(requestID string) context.Context {
	ctx := logging.NewContext(context.Background(), logging.WithRequestID(requestID))
	return ctx
}

func New(notificationQueue notifqueue.Consumer, workersNum int, registry notifregistry.Saver, messenger messenger.Messenger,
	storage storage.Storage) *Sender {

	return &Sender{notificationQueue: notificationQueue, workersNum: workersNum, registry: registry,
		messenger: messenger, storage: storage}
}

func (s *Sender) Start() {
	gLogger.WithField("workers_num", s.workersNum).Info("Listening for notifications")
	for i := 0; i < s.workersNum; i++ {
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
	user := s.getUserFromStorage(ctx, notification.UserID)
	if user == nil {
		logger.WithField("user_id", notification.UserID).Info("Cannot notify not registered user")
		return
	}
	sentMessageIDs := s.sendNotification(ctx, notification, user)
	s.saveToRegistry(ctx, notification, user, sentMessageIDs)
}

func (s *Sender) saveToRegistry(ctx context.Context, notification *models.Notification, user *models.User, messageIDs []int) {

	logger := logging.FromContextAndBase(ctx, gLogger)
	if !user.CanDeleteNotifications {
		logger.WithField("user_id", user.ID).Info("CanDeleteNotifications is false no need to save notifications to the registry")
		return
	}
	for _, messageID := range messageIDs {
		message := &notifregistry.SentNotification{
			UserID:     user.ID,
			FromChatID: notification.ChatID,
			MessageID:  messageID,
			SentAt:     time.Now().UTC(),
		}
		logger = logger.WithField("sent_notification", message)
		logger.Info("Save message sent to the user to the registry")
		err := s.registry.Save(ctx, message)
		if err != nil {
			logger.Errorf("Registry save failed: %s", err)
		}
	}
}

func (s *Sender) getUserFromStorage(ctx context.Context, userID int) *models.User {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("user_id", userID).Info("Get user from storage")
	user, err := s.storage.GetUser(ctx, userID)
	if err != nil {
		logger.Errorf("Storage failed to return user data: %s", err)
		return nil
	}
	return user
}

func (s *Sender) sendNotification(ctx context.Context, notification *models.Notification, user *models.User) []int {
	logger := logging.FromContextAndBase(ctx, gLogger)
	var sentMessageIDs []int
	logger.WithField("user", user).Info("Sending notification to the user")
	textMsgID, err := s.messenger.SendText(ctx, user.PMID, notification.Text)
	if err != nil {
		logger.Errorf("Cannot sent notification text to the user: %s", err)
		return sentMessageIDs
	}
	sentMessageIDs = append(sentMessageIDs, textMsgID)
	forwardMsgID, err := s.messenger.SendForward(ctx, user.PMID, notification.ChatID, notification.MessageID)
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
