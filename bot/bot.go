package bot

import (
	"context"
	"fmt"
	"notifier/config"
	"notifier/incoming"
	"notifier/logging"
	"notifier/models"
	"notifier/neo"
	"notifier/sender"
	"notifier/storage"
	"notifier/tracing"
	"strings"
	"sync"
)

const (
	addLabelCmd    = "addLabel"
	removeLabelCmd = "removeLabel"
	showLabelsCmd  = "showLabels"
)

var (
	gLogger      = logging.WithPackage("bot")
	CommandsText = fmt.Sprintf(`Hi! I can do following for you:\n%s - Add new label for further notifications.\n`+
		`%s - Delete label with provided name.\n%s - Show all your labels.`, addLabelCmd, removeLabelCmd, showLabelsCmd)
)

type Bot struct {
	queue   incomming.Consumer
	neoDB   neo.Client
	sender  sender.Sender
	storage storage.Storage
	wg      sync.WaitGroup
}

func New(queue incomming.Consumer, neoDB neo.Client, sender sender.Sender, storage storage.Storage) *Bot {
	return &Bot{queue: queue, neoDB: neoDB, sender: sender, storage: storage}
}

func prepareContext(msg *models.Message) context.Context {
	requestID := msg.RequestID
	ctx := tracing.NewContext(context.Background(), requestID)
	logger := logging.WithRequestID(requestID)
	ctx = logging.NewContext(ctx, logger)
	return ctx
}

func (b *Bot) Start() {
	conf := config.GetInstance()
	gLogger.WithField("workers_num", conf.WorkersNum).Info("Listening for incoming messages")
	for i := 0; i < conf.WorkersNum; i++ {
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			for {
				gLogger.Debug("Fetching new msg from incoming queue")
				queueMsg, ok := b.queue.GetNext()
				if !ok {
					return
				}
				msg := queueMsg.Payload()
				ctx := prepareContext(msg)
				logger:=logging.FromContextAndBase(ctx,gLogger)
				logger.WithField("msg",msg).Info("Message has been received from incoming queue")
				b.dispatchMessage(ctx, msg)
				logger.Info("Send acknowledgement to the queue")
				queueMsg.Ack()
			}
		}()
	}
}

func (b *Bot) Stop() {
	gLogger.Info("Close incoming queue for reading")
	b.queue.StopGivingMsgs()
	gLogger.Info("Waiting until the workers will process the remaining messages")
	b.wg.Wait()
	gLogger.Info("All workers've been stopped")
}

func (b *Bot) dispatchMessage(ctx context.Context, msg *models.Message) {
	//logger := logging.FromContextAndBase(ctx, gLogger)
	if !msg.Chat.IsPrivate {
		if msg.IsBotAdded {
			b.createChat(ctx, msg.Chat)
			return
		}
		if msg.NewChatMember != nil {
			b.addChatMember(ctx, msg.Chat, msg.NewChatMember.ID)
			return
		}
		if msg.IsBotLeft {
			b.deleteChat(ctx, msg.Chat.ID)
			return
		}
		if msg.LeftChatMember != nil {
			b.removeChatMember(ctx, msg.Chat, msg.LeftChatMember.ID)
			return
		}
		if msg.Text != "" {
			b.regularMessage(ctx, msg)
			return
		}
		return
	}
	cmd, label := msg.ToCommand()
	switch cmd {
	case addLabelCmd:
		b.addUserLabel(ctx, msg.From, label)
	case removeLabelCmd:
		b.removeUserLabel(ctx, msg.From, label)
	case showLabelsCmd:
		b.showUserLabels(ctx, msg.From)
	default:
		b.commandsList(ctx, msg.From)
	}
}

func (b *Bot) commandsList(ctx context.Context, user *models.User) {
	logger := logging.FromContextAndBase(ctx, gLogger)

	err := b.storage.CreateUser(ctx, user)
	if err != nil {
		logger.Error(err)
		return
	}

	logger.Info(CommandsText)
}

func (b *Bot) regularMessage(ctx context.Context, msg *models.Message) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	msgText := processText(msg.Text)

	err := b.storage.CreateChat(ctx, msg.Chat)
	if err != nil {
		logger.Error(err)
		return
	}

	err = b.storage.AddUserToChat(ctx, msg.Chat.ID, msg.From.ID)
	if err != nil {
		logger.Error(err)
		return
	}

	users, err := b.storage.FindUsersByLabel(ctx, msg.Chat.ID, msgText)
	if err != nil {
		logger.Error(err)
		return
	}

	logger.Info(users)
}

func (b *Bot) addChatMember(ctx context.Context, chat *models.Chat, userID int) {
	logger := logging.FromContextAndBase(ctx, gLogger)

	err := b.storage.CreateChat(ctx, chat)
	if err != nil {
		logger.Errorf("Cannot initialize chat: %s", err)
		return
	}

	err = b.storage.AddUserToChat(ctx, chat.ID, userID)
	if err != nil {
		logger.Errorf("Cannot add user chat: %s", err)
		return
	}
}

func (b *Bot) removeChatMember(ctx context.Context, chat *models.Chat, userID int) {
	logger := logging.FromContextAndBase(ctx, gLogger)

	err := b.storage.RemoveUserFromChat(ctx, chat.ID, userID)
	if err != nil {
		logger.Error(err)
		return
	}
}

func (b *Bot) createChat(ctx context.Context, chat *models.Chat) {
	logger := logging.FromContextAndBase(ctx, gLogger)

	err := b.storage.CreateChat(ctx, chat)
	if err != nil {
		logger.Error(err)
		return
	}
}

func (b *Bot) deleteChat(ctx context.Context, chatID int) {
	logger := logging.FromContextAndBase(ctx, gLogger)

	err := b.storage.DeleteChat(ctx, chatID)
	if err != nil {
		logger.Error(err)
		return
	}
}

func (b *Bot) addUserLabel(ctx context.Context, user *models.User, label string) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	label = processText(label)

	err := b.storage.CreateUser(ctx, user)
	if err != nil {
		logger.Error(err)
		return
	}

	err = b.storage.AddLabelToUser(ctx, user.ID, label)
	if err != nil {
		logger.Error(err)
		return
	}
}

func (b *Bot) removeUserLabel(ctx context.Context, user *models.User, label string) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	label = processText(label)

	err := b.storage.CreateUser(ctx, user)
	if err != nil {
		logger.Error(err)
		return
	}

	err = b.storage.RemoveLabelFromUser(ctx, user.ID, label)
	if err != nil {
		logger.Error(err)
		return
	}
}

func (b *Bot) showUserLabels(ctx context.Context, user *models.User) {
	logger := logging.FromContextAndBase(ctx, gLogger)

	err := b.storage.CreateUser(ctx, user)
	if err != nil {
		logger.Error(err)
		return
	}

	labels, err := b.storage.GetUserLabels(ctx, user.ID)
	if err != nil {
		logger.Error(err)
		return
	}

	logger.Info(labels)
}

func processText(text string) string {
	return strings.ToLower(strings.TrimSpace(text))
}
