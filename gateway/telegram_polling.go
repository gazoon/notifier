package gateway

import (
	"notifier/config"
	"notifier/libs/logging"
	"notifier/libs/models"
	"time"

	"context"

	"notifier/libs/queue/messages"

	"github.com/go-telegram-bot-api/telegram-bot-api"
	"github.com/pkg/errors"
)

var (
	gLogger = logging.WithPackage("telegram_polling")
)

func transformUser(u *tgbotapi.User) *models.User {
	if u == nil {
		return nil
	}
	name := u.FirstName
	if u.LastName != "" {
		name += " " + u.LastName
	}
	return &models.User{
		ID:       u.ID,
		Name:     name,
		Username: u.UserName,
	}
}

func prepareContext(requestID string) context.Context {
	logger := logging.WithRequestID(requestID)
	ctx := logging.NewContext(context.Background(), logger)
	return ctx
}

type TelegramPoller struct {
	queue   msgsqueue.Producer
	botName string
}

func NewTelegramPoller(queue msgsqueue.Producer, botName string) *TelegramPoller {
	return &TelegramPoller{queue: queue, botName: botName}
}

func (tp *TelegramPoller) userIsBot(u *tgbotapi.User) bool {
	if u == nil {
		return false
	}
	return u.UserName == tp.botName
}

func (tp *TelegramPoller) updateMessageToModel(updateMessage *tgbotapi.Message) (*models.Message, error) {
	if updateMessage.Chat == nil {
		return nil, errors.New("message without chat")
	}
	if updateMessage.From == nil {
		return nil, errors.New("message without from")
	}
	var voice *models.Voice
	if updateMessage.Voice != nil {
		var voiceSize *int
		size := updateMessage.Voice.FileSize
		if size != 0 {
			voiceSize = &size
		}
		voice = &models.Voice{ID: updateMessage.Voice.FileID, Duration: updateMessage.Voice.Duration, Size: voiceSize}
	}
	chatID := int(updateMessage.Chat.ID)
	message := &models.Message{
		ID:    updateMessage.MessageID,
		Text:  updateMessage.Text,
		Voice: voice,
		Chat: &models.Chat{
			ID:        chatID,
			IsPrivate: updateMessage.Chat.IsPrivate(),
			Title:     updateMessage.Chat.Title,
		},
		From:       transformUser(updateMessage.From),
		IsBotAdded: tp.userIsBot(updateMessage.NewChatMember),
		IsBotLeft:  tp.userIsBot(updateMessage.LeftChatMember),
	}
	if !message.IsBotAdded {
		message.NewChatMember = transformUser(updateMessage.NewChatMember)
	}
	if !message.IsBotLeft {
		message.LeftChatMember = transformUser(updateMessage.LeftChatMember)
	}
	if updateMessage.Chat.IsPrivate() {
		message.From.PMID = chatID
	}
	return message, nil
}

func (tp *TelegramPoller) processUpdate(update *tgbotapi.Update) {
	requestID := logging.NewRequestID()
	ctx := prepareContext(requestID)
	logger := logging.FromContextAndBase(ctx, gLogger)
	if update.Message == nil {
		logger.Debugf("Skip update without the Message field: %+v", update)
		return
	}
	msg, err := tp.updateMessageToModel(update.Message)
	if err != nil {
		logger.Warnf("Cannot transform telegram update %+v to message: %s", update, err)
		return
	}
	msg.RequestID = requestID
	msg.CreatedAt = time.Now()
	logger.WithField("msg", msg).Info("Put a new msg in the incoming queue")
	err = tp.queue.Put(ctx, msg)
	if err == msgsqueue.DuplicateMsgErr {
		logger.Warn("Message already exists in the incoming queue, skip")
	} else if err != nil {
		logger.Errorf("Cannot put incoming msg: %s", err)
	}
}

func (tp *TelegramPoller) Start() error {
	conf := config.GetInstance()
	bot, err := tgbotapi.NewBotAPI(conf.Telegram.APIToken)
	if err != nil {
		return errors.Wrap(err, "telegram api initialization failed")
	}
	gLogger.Info("Starting polling for updates")
	retryDelay := conf.TelegramPolling.RetryDelay
	updatesConf := tgbotapi.UpdateConfig{Timeout: conf.TelegramPolling.PollTimeout}
	go func() {
		for {
			gLogger.Debug("Requesting new updates from API")
			updates, err := bot.GetUpdates(updatesConf)
			if err != nil {
				gLogger.Warnf("Failed to get updates: %s retrying in %d seconds...", err, retryDelay)
				time.Sleep(time.Second * time.Duration(retryDelay))
				continue
			}
			gLogger.Info("Telegram API returns updates: %v", updates)
			for i := range updates {
				update := &updates[i]
				if update.UpdateID >= updatesConf.Offset {
					updatesConf.Offset = update.UpdateID + 1
					tp.processUpdate(update)
				}
			}
		}
	}()
	return nil
}
