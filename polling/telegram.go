package polling

import (
	"notifier/config"
	"sync"

	"notifier/logging"
	"notifier/models"
	"time"

	"notifier/incoming"
	"notifier/tracing"

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

func updateToMessage(update *tgbotapi.Update) (*models.Message, error) {
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
		From:       transformUser(update.Message.From),
		IsBotAdded: userIsBot(update.Message.NewChatMember),
		IsBotLeft:  userIsBot(update.Message.LeftChatMember),
	}
	if !message.IsBotAdded {
		message.NewChatMember = transformUser(update.Message.NewChatMember)
	}
	if !message.IsBotLeft {
		message.LeftChatMember = transformUser(update.Message.LeftChatMember)
	}
	if update.Message.Chat.IsPrivate() {
		message.From.PMID = chatID
	}
	return message, nil

}

func sendUpdate(update *tgbotapi.Update) {
	requestID := tracing.NewRequestID()
	logger := logging.WithRequestIDAndBase(requestID, gLogger)
	msg, err := updateToMessage(update)
	if err != nil {
		logger.Errorf("Cannot transform telegram update to message: %s", err)
		return
	}
	msg.RequestID = requestID
	queue := incomming.GetQueue()
	err = queue.Put(msg)
	if err != nil {
		logger.Errorf("Cannot put incoming msg: %s", err)
		return
	}
}

type TelegramPoller struct {
	wg        sync.WaitGroup
	isStopped bool
}

func (tp *TelegramPoller) Start() error {
	if tp.isStopped {
		return errors.New("poller is stopped")
	}
	conf := config.GetInstance()
	bot, err := tgbotapi.NewBotAPI(conf.Telegram.APIToken)
	if err != nil {
		return errors.Wrap(err, "telegram api initialization failed")
	}
	tp.wg.Add(1)
	gLogger.Info("Starting polling for updates")
	retryDelay := conf.TelegramPolling.RetryDelay
	updatesConf := tgbotapi.UpdateConfig{Timeout: conf.TelegramPolling.PollTimeout}
	go func() {
		defer tp.wg.Done()
		for {
			if tp.isStopped {
				return
			}
			gLogger.Debug("Requesting new updates from API")
			updates, err := bot.GetUpdates(updatesConf)
			if err != nil {
				gLogger.Errorf("Failed to get updates: %s retrying in %d seconds...", err, retryDelay)
				time.Sleep(time.Second * time.Duration(retryDelay))
				continue
			}
			for i := range updates {
				update := &updates[i]
				if update.UpdateID >= updatesConf.Offset {
					updatesConf.Offset = update.UpdateID + 1
					sendUpdate(update)
				}
			}
		}
	}()
	return nil
}

func (tp *TelegramPoller) Stop() {
	tp.isStopped = true
	gLogger.Info("Stoping polling telegram API")
	tp.wg.Wait()
}
