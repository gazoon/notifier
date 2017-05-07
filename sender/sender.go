package sender

import (
	"context"
	"net/http"
	"notifier/logging"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/go-telegram-bot-api/telegram-bot-api"
	"github.com/pkg/errors"
)

var (
	gLogger = logging.WithPackage("sender")
)

type Sender interface {
	SendText(ctx context.Context, chatID int, text string)
	SendForward(ctx context.Context, toChatID, fromChatID, msgID int)
	SendForwardWithText(ctx context.Context, toChatID, fromChatID, msgID int, text string)
}

type telegramSender struct {
	bot *tgbotapi.BotAPI
}

func NewTelegramSender(apiToken string, httpTimeout int) (Sender, error) {
	timeout := time.Duration(httpTimeout) * time.Second
	bot, err := tgbotapi.NewBotAPIWithClient(apiToken, &http.Client{Timeout: timeout})
	if err != nil {
		return nil, errors.Wrap(err, "telegram api failed")
	}
	return &telegramSender{bot}, nil
}

func (ts *telegramSender) SendText(ctx context.Context, chatID int, text string) {
	logger := logging.FromContextAndBase(ctx, gLogger).WithFields(log.Fields{"chat_id": chatID, "text": text})
	msg := tgbotapi.NewMessage(int64(chatID), text)
	logger.Debug("Sending text to the chat")
	_, err := ts.bot.Send(msg)
	if err != nil {
		logger.Errorf("Cannot send text to telegram API: %s", err)
		return
	}
}

func (ts *telegramSender) SendForward(ctx context.Context, toChatID, fromChatID, msgID int) {
	logger := logging.FromContextAndBase(ctx, gLogger).WithField("msg_id", msgID)
	forward := tgbotapi.NewForward(int64(toChatID), int64(fromChatID), msgID)
	logger.Debug("Forwarding msg to the chat")
	_, err := ts.bot.Send(forward)
	if err != nil {
		logger.Errorf("Cannot forward msg to telegram API: %s", err)
		return
	}
}

func (ts *telegramSender) SendForwardWithText(ctx context.Context, toChatID, fromChatID, msgID int, text string) {
	ts.SendText(ctx, toChatID, text)
	ts.SendForward(ctx, toChatID, fromChatID, msgID)
}
