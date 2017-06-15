package messenger

import (
	"context"
	"net/http"
	"notifier/libs/logging"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/go-telegram-bot-api/telegram-bot-api"
	"github.com/pkg/errors"
	"io/ioutil"
)

const (
	userLeftStatus   = "left"
	userKickedStatus = "kicked"
)

var (
	gLogger = logging.WithPackage("messenger")
)

type Messenger interface {
	SendText(ctx context.Context, chatID int, text string) (int, error)
	SendReply(ctx context.Context, chatID, msgID int, text string) (int, error)
	SendForward(ctx context.Context, toChatID, fromChatID, msgID int) (int, error)
	IsUserInChat(ctx context.Context, userID, chatID int) (bool, error)
	DeleteMessage(ctx context.Context, messageID, chatID int) error
	DownloadFile(ctx context.Context, fileID string) ([]byte, error)
}

type telegram struct {
	bot *tgbotapi.BotAPI
}

func NewTelegram(apiToken string, httpTimeout int) (Messenger, error) {
	timeout := time.Duration(httpTimeout) * time.Second
	bot, err := tgbotapi.NewBotAPIWithClient(apiToken, &http.Client{Timeout: timeout})
	if err != nil {
		return nil, errors.Wrap(err, "telegram api failed")
	}
	return &telegram{bot}, nil
}

func (ts *telegram) SendText(ctx context.Context, chatID int, text string) (int, error) {
	logger := logging.FromContextAndBase(ctx, gLogger).WithFields(log.Fields{"chat_id": chatID, "text": text})
	msg := tgbotapi.NewMessage(int64(chatID), text)
	logger.Info("Calling send text API method")
	sentMessage, err := ts.bot.Send(msg)
	if err != nil {
		return 0, errors.Wrap(err, "cannot send text to telegram API")
	}
	return sentMessage.MessageID, nil
}

func (ts *telegram) SendReply(ctx context.Context, chatID, msgID int, text string) (int, error) {
	logger := logging.FromContextAndBase(ctx, gLogger).WithFields(log.Fields{"chat_id": chatID, "msg_id": msgID, "text": text})
	msg := tgbotapi.NewMessage(int64(chatID), text)
	msg.ReplyToMessageID = msgID
	logger.Info("Calling send reply API method")
	sentMessage, err := ts.bot.Send(msg)
	if err != nil {
		return 0, errors.Wrap(err, "cannot send reply to telegram API")
	}
	return sentMessage.MessageID, nil
}

func (ts *telegram) SendForward(ctx context.Context, toChatID, fromChatID, msgID int) (int, error) {
	logger := logging.FromContextAndBase(ctx, gLogger).WithField("msg_id", msgID)
	forward := tgbotapi.NewForward(int64(toChatID), int64(fromChatID), msgID)
	logger.Info("Calling forward message API method")
	sentMessage, err := ts.bot.Send(forward)
	if err != nil {
		return 0, errors.Wrap(err, "cannot forward msg to telegram API")
	}
	return sentMessage.MessageID, nil
}

func (ts *telegram) IsUserInChat(ctx context.Context, userID, chatID int) (bool, error) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	args := tgbotapi.ChatConfigWithUser{ChatID: int64(chatID), UserID: userID}
	logger.Infof("Calling get chat member info API method, args: %+v", args)
	memberInfo, err := ts.bot.GetChatMember(args)
	if err != nil {
		if isUserNotInChatErr(err) {
			return false, nil
		}
		return false, errors.Wrap(err, "cannot request info about user in chat")
	}
	userStatus := memberInfo.Status
	return userStatus != userLeftStatus && userStatus != userKickedStatus, nil
}

func (ts *telegram) DownloadFile(ctx context.Context, fileID string) ([]byte, error) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("file_id", fileID).Info("Retrieve file url from telegram")

	fileURL, err := ts.bot.GetFileDirectURL(fileID)
	if err != nil {
		return nil, errors.Wrap(err, "file url retrieving failed")
	}

	logger.WithField("file_url", fileURL).Info("Download file from telegram storage")
	resp, err := ts.bot.Client.Get(fileURL)
	if err != nil {
		return nil, errors.Wrap(err, "cannot GET file")
	}
	defer resp.Body.Close()

	content, err := ioutil.ReadAll(resp.Body)
	return content, errors.Wrap(err, "file content reading failed")
}

func (ts *telegram) DeleteMessage(ctx context.Context, messageID, chatID int) error {
	logger := logging.FromContextAndBase(ctx, gLogger)
	args := tgbotapi.DeleteMessageConfig{ChatID: int64(chatID), MessageID: messageID}
	logger.Infof("Calling delte message api method, args: %+v", args)
	resp, err := ts.bot.DeleteMessage(args)
	if err != nil {
		return errors.Wrap(err, "delete api failed")
	}
	logger.Infof("Delete api response: %+v", resp)
	return nil
}

func isUserNotInChatErr(err error) bool {
	return http.StatusText(http.StatusBadRequest) == err.Error()
}
