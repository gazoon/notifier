package telegram

import (
	"notifier/config"
	"sync"

	"context"
	"notifier/logging"
	"notifier/models"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/go-telegram-bot-api/telegram-bot-api"
	"github.com/pkg/errors"
)

var (
	once    sync.Once
	gLogger = logging.WithPackage("telegram")
)
var (
	bot          *tgbotapi.BotAPI
	MessagesChan chan *models.Message
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

func updateToMessage(update tgbotapi.Update) (*models.Message, error) {
	if update.Message == nil {
		return nil, errors.New("update without message")
	}
	if update.Message.Chat == nil {
		return nil, errors.New("message without chat")
	}
	if update.Message.From == nil {
		return nil, errors.New("message without from")
	}
	chatID := int(update.Message.Chat.ID)
	message := &models.Message{
		ID:   update.Message.MessageID,
		Text: update.Message.Text,
		Chat: &models.Chat{
			ID:        chatID,
			IsPrivate: update.Message.Chat.IsPrivate(),
			Title:     update.Message.Chat.Title,
		},
		From:           transformUser(update.Message.From),
		NewChatMember:  transformUser(update.Message.NewChatMember),
		LeftChatMember: transformUser(update.Message.LeftChatMember),
	}
	if update.Message.Chat.IsPrivate() {
		message.From.PMID = chatID
	}
	return message, nil

}
func createMessagesChan() chan *models.Message {
	ch := make(chan *models.Message, config.Telegram.ChannelSize)
	updatesConf := tgbotapi.UpdateConfig{Timeout: config.Telegram.PullTimeout}
	retryDelay := config.Telegram.RetryDelay
	go func() {
		for {
			updates, err := bot.GetUpdates(updatesConf)
			if err != nil {
				gLogger.Errorf("Failed to get updates: %s retrying in %d seconds...", err, retryDelay)
				time.Sleep(time.Second * time.Duration(retryDelay))
				continue
			}

			for _, update := range updates {
				if update.UpdateID >= updatesConf.Offset {
					updatesConf.Offset = update.UpdateID + 1
					msg, err := updateToMessage(update)
					if err != nil {
						gLogger.Errorf("Cannot transform api update to message: %s", err)
					}
					ch <- msg
				}
			}
		}
	}()
	return ch
}

func Initialization() {
	once.Do(func() {
		var err error
		bot, err = tgbotapi.NewBotAPI(config.Telegram.APIToken)
		if err != nil {
			panic(errors.WithMessage(err, "cannot initialize telegram api"))
		}
		MessagesChan = createMessagesChan()
	})
}

func getLogger(ctx context.Context) *log.Entry {
	return logging.FromContextAndBase(ctx, gLogger)
}

func SendText(ctx context.Context, chatID int, text string) {
	msg := tgbotapi.NewMessage(int64(chatID), text)
	_, err := bot.Send(msg)
	if err != nil {
		logger := getLogger(ctx)
		logger.Errorf("Cannot send text to telegram API: %s", err)
	}
}

func SendForward(ctx context.Context, toChatID, fromChatID, msgID int) {
	forward := tgbotapi.NewForward(int64(toChatID), int64(fromChatID), msgID)
	_, err := bot.Send(forward)
	if err != nil {
		logger := getLogger(ctx)
		logger.Errorf("Cannot forward msg to telegram API: %s", err)
	}
}

func SendForwardWithText(ctx context.Context, toChatID, fromChatID, msgID int, text string) {
	SendText(ctx, toChatID, text)
	SendForward(ctx, toChatID, fromChatID, msgID)
}
