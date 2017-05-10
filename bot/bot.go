package bot

import (
	"context"
	"fmt"
	"notifier/config"
	"notifier/incoming"
	"notifier/logging"
	"notifier/messenger"
	"notifier/models"
	"notifier/neo"
	"notifier/storage"
	"notifier/tracing"
	"strings"
	"sync"

	log "github.com/Sirupsen/logrus"
)

const (
	addLabelCmd    = "addLabel"
	removeLabelCmd = "removeLabel"
	showLabelsCmd  = "showLabels"
)

var (
	gLogger      = logging.WithPackage("bot")
	commandsText = fmt.Sprintf(`Hi! I can do following for you:\n%s - Add new label for further notifications.\n`+
		`%s - Delete label with provided name.\n%s - Show all your labels.`, addLabelCmd, removeLabelCmd, showLabelsCmd)
	notificationTextTemplate = `%s, you've been mentioned in the %s:`
	errorText                = `An internal bot error occurred.`
	noLabelsText             = `You don't have any labels yet.`
)

type Bot struct {
	queue     incomming.Consumer
	neoDB     neo.Client
	messenger messenger.Messenger
	storage   storage.Storage
	wg        sync.WaitGroup
}

func New(queue incomming.Consumer, neoDB neo.Client, sender messenger.Messenger, storage storage.Storage) *Bot {
	return &Bot{queue: queue, neoDB: neoDB, messenger: sender, storage: storage}
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
				gLogger.Info("Fetching new msg from incoming queue")
				queueMsg, ok := b.queue.GetNext()
				if !ok {
					return
				}
				msg := queueMsg.Payload()
				ctx := prepareContext(msg)
				logger := logging.FromContextAndBase(ctx, gLogger)
				logger.WithField("msg", msg).Info("Message has been received from incoming queue")
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
			b.createChatHandler(ctx, msg.Chat)
			return
		}
		if msg.NewChatMember != nil {
			b.addChatMemberHandler(ctx, msg.Chat, msg.NewChatMember.ID)
			return
		}
		if msg.IsBotLeft {
			b.deleteChatHandler(ctx, msg.Chat.ID)
			return
		}
		if msg.LeftChatMember != nil {
			b.removeChatMemberHandler(ctx, msg.Chat, msg.LeftChatMember.ID)
			return
		}
		if msg.Text != "" {
			b.regularMessageHandler(ctx, msg)
			return
		}
		return
	}
	cmd, label := msg.ToCommand()
	switch cmd {
	case addLabelCmd:
		b.addUserLabelHandler(ctx, msg.From, label)
	case removeLabelCmd:
		b.removeUserLabelHandler(ctx, msg.From, label)
	case showLabelsCmd:
		b.showUserLabelsHandler(ctx, msg.From)
	default:
		b.commandsListHandler(ctx, msg.From)
	}
}

func (b *Bot) createUser(ctx context.Context, user *models.User) bool {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("user", user).Info("Saving user in the storage")
	err := b.storage.CreateUser(ctx, user)
	if err != nil {
		logger.Errorf("Cannot save user in the storage: %s", err)
		return false
	}
	return true
}

func (b *Bot) createChat(ctx context.Context, chat *models.Chat) bool {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("chat", chat).Info("Saving chat in the storage")
	err := b.storage.CreateChat(ctx, chat)
	if err != nil {
		logger.Errorf("Cannot save chat in the storage: %s", err)
		return false
	}
	return true
}

func (b *Bot) addUserToChat(ctx context.Context, chatID, userID int) bool {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithFields(log.Fields{"chat_id": chatID, "user_id": userID}).Info("Saving user-chat relation in the storage")
	err := b.storage.AddUserToChat(ctx, chatID, userID)
	if err != nil {
		logger.Errorf("Cannot save user-chat: %s", err)
		return false
	}
	return true
}

func (b *Bot) notifyUsers(ctx context.Context, users []*models.User, msg *models.Message) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	for _, user := range users {
		notificationText := fmt.Sprintf(notificationTextTemplate, user.Name, msg.Chat.Title)
		logger.WithField("user", user).Info("Sending notification to the user")
		err := b.messenger.SendForwardWithText(ctx, user.PMID, msg.Chat.ID, msg.ID, notificationText)
		if err != nil {
			logger.Errorf("Cannot notify user: %s", err)
			continue
		}
	}
}

func (b *Bot) sendErrorMsg(ctx context.Context, user *models.User) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("user", user).Info("Sending error msg to the user")
	err := b.messenger.SendText(ctx, user.PMID, errorText)
	if err != nil {
		logger.Errorf("Cannot send error msg: %s", err)
		return
	}
}

func (b *Bot) sendUserLabels(ctx context.Context, user *models.User, labels []string) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	var labelsText string
	if len(labels) != 0 {
		labelsText = strings.Join(labels, "\n")
	} else {
		labelsText = noLabelsText
	}
	logger.WithField("labels_text", labelsText).Info("Sending labels to the user")
	err := b.messenger.SendText(ctx, user.PMID, labelsText)
	if err != nil {
		logger.Errorf("Cannot send list of user labels: %s", err)
		return
	}
}

func (b *Bot) filterNotChatUsers(ctx context.Context, users []*models.User, chat *models.Chat) []*models.User {
	logger := logging.FromContextAndBase(ctx, gLogger).WithField("chat_id", chat.ID)
	var filteredUsers []*models.User
	for _, user := range users {
		userLogger := logger.WithField("user_id", user.ID)
		userLogger.Info("Check in the messenger whether the user is still in the chat")
		isInChat, err := b.messenger.IsUserInChat(ctx, user.ID, chat.ID)
		if err != nil {
			userLogger.Errorf("Cannot get info about the user in the chat from the messenger: %s", err)
			continue
		}
		if isInChat {
			filteredUsers = append(filteredUsers, user)
			continue
		}
		userLogger.Info("The user is no longer in the chat, discarding from the storage")
		err = b.storage.RemoveUserFromChat(ctx, chat.ID, user.ID)
		if err != nil {
			userLogger.Errorf("Cannot remove not actual user-chat relation from the storage: %s", err)
			continue
		}
	}
	return filteredUsers
}

func (b *Bot) commandsListHandler(ctx context.Context, user *models.User) {
	logger := logging.FromContextAndBase(ctx, gLogger)

	b.createUser(ctx, user)

	logger.WithField("user_id", user.ID).Info("Sending the list of commands")
	err := b.messenger.SendText(ctx, user.PMID, commandsText)
	if err != nil {
		logger.Errorf("cannot send commands to the user: %s", err)
	}
}

func (b *Bot) regularMessageHandler(ctx context.Context, msg *models.Message) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	msgText := processText(msg.Text)

	if !b.createChat(ctx, msg.Chat) {
		return
	}

	b.addUserToChat(ctx, msg.Chat.ID, msg.From.ID)

	logger.WithField("message_text", msgText).Info("Searching for users mentioned in the message text")
	users, err := b.storage.FindUsersByLabel(ctx, msg.Chat.ID, msgText)
	if err != nil {
		logger.Errorf("Cannot get users from storage to notify: %s", err)
		return
	}

	users = b.filterNotChatUsers(ctx, users, msg.Chat)
	b.notifyUsers(ctx, users, msg)
}

func (b *Bot) addChatMemberHandler(ctx context.Context, chat *models.Chat, userID int) {
	if !b.createChat(ctx, chat) {
		return
	}

	b.addUserToChat(ctx, chat.ID, userID)
}

func (b *Bot) removeChatMemberHandler(ctx context.Context, chat *models.Chat, userID int) {
	logger := logging.FromContextAndBase(ctx, gLogger)

	logger.WithFields(log.Fields{"user_id": userID, "chat_id": chat.ID}).Info("Remove user-chat relation")
	err := b.storage.RemoveUserFromChat(ctx, chat.ID, userID)
	if err != nil {
		logger.Errorf("Cannot remove user-chat relation from the storage: %s", err)
		return
	}
}

func (b *Bot) createChatHandler(ctx context.Context, chat *models.Chat) {
	b.createChat(ctx, chat)
}

func (b *Bot) deleteChatHandler(ctx context.Context, chatID int) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("chat_id", chatID).Info("Delete chat entry from the storage")
	err := b.storage.DeleteChat(ctx, chatID)
	if err != nil {
		logger.Errorf("Cannot remove chat from the storage: %s", err)
		return
	}
}

func (b *Bot) addUserLabelHandler(ctx context.Context, user *models.User, label string) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	label = processText(label)

	if !b.createUser(ctx, user) {
		b.sendErrorMsg(ctx, user)
		return
	}

	logger.WithFields(log.Fields{"label": label, "user_id": user.ID}).Info("Saving new user label in the storage")
	err := b.storage.AddLabelToUser(ctx, user.ID, label)
	if err != nil {
		logger.Errorf("Cannot save user label: %s", err)
		b.sendErrorMsg(ctx, user)
		return
	}
}

func (b *Bot) removeUserLabelHandler(ctx context.Context, user *models.User, label string) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	label = processText(label)

	if !b.createUser(ctx, user) {
		b.sendErrorMsg(ctx, user)
		return
	}

	logger.WithFields(log.Fields{"label": label, "user_id": user.ID}).Info("Discarding user label from the storage")
	err := b.storage.RemoveLabelFromUser(ctx, user.ID, label)
	if err != nil {
		logger.Errorf("Removing user label from the storage failed: %s", err)
		b.sendErrorMsg(ctx, user)
		return
	}
}

func (b *Bot) showUserLabelsHandler(ctx context.Context, user *models.User) {
	logger := logging.FromContextAndBase(ctx, gLogger)

	if !b.createUser(ctx, user) {
		b.sendErrorMsg(ctx, user)
		return
	}

	logger.WithField("user_id", user.ID).Info("Get user labels from the storage")
	labels, err := b.storage.GetUserLabels(ctx, user.ID)
	if err != nil {
		logger.Errorf("Cannot fetch a list of user labels from storage: %s", err)
		b.sendErrorMsg(ctx, user)
		return
	}

	b.sendUserLabels(ctx, user, labels)
}

func processText(text string) string {
	return strings.ToLower(strings.TrimSpace(text))
}
