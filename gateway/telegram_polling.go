package gateway

import (
	"notifier/config"
	"notifier/logging"
	"notifier/models"
	"time"

	"notifier/incoming"
	"notifier/tracing"

	"context"

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

func userIsBot(u *tgbotapi.User) bool {
	if u == nil {
		return false
	}
	conf := config.GetInstance()
	return u.UserName == conf.Telegram.BotName
}

func updateMessageToModel(updateMessage *tgbotapi.Message) (*models.Message, error) {
	if updateMessage.Chat == nil {
		return nil, errors.New("message without chat")
	}
	if updateMessage.From == nil {
		return nil, errors.New("message without from")
	}
	chatID := int(updateMessage.Chat.ID)
	message := &models.Message{
		ID:   updateMessage.MessageID,
		Text: updateMessage.Text,
		Chat: &models.Chat{
			ID:        chatID,
			IsPrivate: updateMessage.Chat.IsPrivate(),
			Title:     updateMessage.Chat.Title,
		},
		From:       transformUser(updateMessage.From),
		IsBotAdded: userIsBot(updateMessage.NewChatMember),
		IsBotLeft:  userIsBot(updateMessage.LeftChatMember),
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

func prepareContext(requestID string) context.Context {
	logger := logging.WithRequestID(requestID)
	ctx := logging.NewContext(context.Background(), logger)
	return ctx
}

type TelegramPoller struct {
	queue incomming.Producer
}

func NewTelegramPoller(queue incomming.Producer) *TelegramPoller {
	return &TelegramPoller{queue}
}

func (tp *TelegramPoller) processUpdate(update *tgbotapi.Update) {
	requestID := tracing.NewRequestID()
	ctx := prepareContext(requestID)
	logger := logging.FromContextAndBase(ctx, gLogger)
	if update.Message == nil {
		logger.Debugf("Skip update without the Message field: %+v", update)
		return
	}
	msg, err := updateMessageToModel(update.Message)
	if err != nil {
		logger.Warnf("Cannot transform telegram update %+v to message: %s", update, err)
		return
	}
	msg.RequestID = requestID
	logger.WithField("msg", msg).Info("Put a new msg in the incoming queue")
	err = tp.queue.Put(ctx, msg)
	if err != nil {
		logger.Errorf("Cannot put incoming msg: %s", err)
		return
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
