package bot

import (
	"context"
	"fmt"
	"notifier/config"
	"notifier/logging"
	"notifier/messages_queue"
	"notifier/messenger"
	"notifier/models"
	"notifier/neo"
	"notifier/storage"
	"notifier/tracing"
	"strings"
	"sync"

	log "github.com/Sirupsen/logrus"
	"notifier/notifications_queue"
)

const (
	notificationDelay = 100
	addLabelCmd       = "addLabel"
	removeLabelCmd    = "removeLabel"
	showLabelsCmd     = "showLabels"
)

var (
	gLogger      = logging.WithPackage("bot")
	commandsText = fmt.Sprintf("Hi! I can do following for you:\n%s - Add new label for further notifications.\n"+
		"%s - Delete label with provided name.\n%s - Show all your labels.", addLabelCmd, removeLabelCmd, showLabelsCmd)
	notificationTextTemplate   = "%s, you've been mentioned in the %s chat:"
	errorText                  = "An internal bot error occurred."
	noLabelsText               = "You don't have any labels yet."
	labelArgMissedTextTemplate = "You didn't provide a label\nEnter %s {label_name}"
	okText                     = "OK."
)

func prepareContext(msg *models.Message) context.Context {
	requestID := msg.RequestID
	ctx := tracing.NewContext(context.Background(), requestID)
	logger := logging.WithRequestID(requestID)
	ctx = logging.NewContext(ctx, logger)
	return ctx
}

type Handler func(ctx context.Context, msg *models.Message)

type Bot struct {
	messagesQueue     msgsqueue.Consumer
	notificationQueue notifqueue.Producer
	neoDB             neo.Client
	messenger         messenger.Messenger
	storage           storage.Storage
	wg                sync.WaitGroup
}

func New(messagesQueue msgsqueue.Consumer, notificationQueue notifqueue.Producer, neoDB neo.Client,
	messenger messenger.Messenger, storage storage.Storage) *Bot {

	return &Bot{
		messagesQueue:     messagesQueue,
		notificationQueue: notificationQueue,
		neoDB:             neoDB,
		messenger:         messenger,
		storage:           storage,
	}
}

func (b *Bot) Start() {
	conf := config.GetInstance()
	gLogger.WithField("workers_num", conf.BotWorkersNum).Info("Listening for incoming messages")
	for i := 0; i < conf.BotWorkersNum; i++ {
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			for {
				gLogger.Info("Fetching new msg from incoming queue")
				queueMsg, ok := b.messagesQueue.GetNext()
				if !ok {
					return
				}
				msg := queueMsg.Payload()
				ctx := prepareContext(msg)
				logger := logging.FromContextAndBase(ctx, gLogger)
				logger.WithField("msg", msg).Info("Message received from incoming queue")
				b.dispatchMessage(ctx, msg)
				logger.Info("Send acknowledgement to the queue")
				queueMsg.Ack()
			}
		}()
	}
}

func (b *Bot) Stop() {
	gLogger.Info("Close incoming queue for reading")
	b.messagesQueue.StopGivingMsgs()
	gLogger.Info("Waiting until all workers will process the remaining messages")
	b.wg.Wait()
	gLogger.Info("All workers've been stopped")
}

func (b *Bot) dispatchMessage(ctx context.Context, msg *models.Message) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	var handlerFunc Handler
	var handlerName string
	if !msg.Chat.IsPrivate {
		switch {
		case msg.IsBotAdded:
			handlerFunc = b.createChatHandler
			handlerName = "createChat"
		case msg.NewChatMember != nil:
			handlerFunc = b.addChatMemberHandler
			handlerName = "addChatMember"
		case msg.IsBotLeft:
			handlerFunc = b.deleteChatHandler
			handlerName = "deleteChat"
		case msg.LeftChatMember != nil:
			handlerFunc = b.removeChatMemberHandler
			handlerName = "removeChatMember"
		default:
			handlerFunc = b.regularMessageHandler
			handlerName = "regularMessage"
		}
	} else {
		cmd, _ := msg.ToCommand()
		switch cmd {
		case addLabelCmd:
			handlerFunc = b.addUserLabelHandler
			handlerName = "addUserLabel"
		case removeLabelCmd:
			handlerFunc = b.removeUserLabelHandler
			handlerName = "removeUserLabel"
		case showLabelsCmd:
			handlerFunc = b.showUserLabelsHandler
			handlerName = "showUserLabels"
		default:
			handlerFunc = b.commandsListHandler
			handlerName = "commandsList"
		}
	}
	logger.WithField("handler_name", handlerName).Info("Calling handler")
	handlerFunc(ctx, msg)
}

func (b *Bot) createUser(ctx context.Context, user *models.User) bool {
	logger := logging.FromContextAndBase(ctx, gLogger)
	labelFromName := processText(user.Name)
	logger.WithField("user", user).Info("Saving user in the storage")
	err := b.storage.CreateUser(ctx, user, []string{labelFromName})
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
		notification := models.NewNotification(user, msg.ID, msg.Chat.ID, notificationDelay, notificationText,
			msg.RequestID)
		logger.WithField("notification", notification).Info("Put notification to the queue")
		err := b.notificationQueue.Put(ctx, notification)
		if err != nil {
			logger.Errorf("Cannot save user notification: %s", err)
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

func (b *Bot) sendOKMsg(ctx context.Context, user *models.User) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("user", user).Info("Sending ok message")
	err := b.messenger.SendText(ctx, user.PMID, okText)
	if err != nil {
		logger.Errorf("Cannot send ok msg to the user %s", err)
		return
	}
}

func (b *Bot) sendMissLabelArgMsg(ctx context.Context, user *models.User, cmd string) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("user", user).Infof("Call cmd %s without label arg, sending label missed message", cmd)
	labelArgMissedText := fmt.Sprintf(labelArgMissedTextTemplate, cmd)
	err := b.messenger.SendText(ctx, user.PMID, labelArgMissedText)
	if err != nil {
		logger.Errorf("Cannot send label missed msg: %s", err)
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

func (b *Bot) commandsListHandler(ctx context.Context, msg *models.Message) {
	user := msg.From
	logger := logging.FromContextAndBase(ctx, gLogger)

	b.createUser(ctx, user)

	logger.WithField("user_id", user.ID).Info("Sending the list of commands")
	err := b.messenger.SendText(ctx, user.PMID, commandsText)
	if err != nil {
		logger.Errorf("cannot send commands to the user: %s", err)
	}
}

func (b *Bot) regularMessageHandler(ctx context.Context, msg *models.Message) {
	conf := config.GetInstance()
	logger := logging.FromContextAndBase(ctx, gLogger)
	msgText := processText(msg.Text)

	if !b.createChat(ctx, msg.Chat) {
		return
	}

	b.addUserToChat(ctx, msg.Chat.ID, msg.From.ID)

	logger.WithFields(log.Fields{"user": msg.From, "chat_id": msg.Chat.ID}).
		Info("Discarding not actual user notifications")
	err := b.notificationQueue.Discard(ctx, msg.From, msg.Chat.ID)
	if err != nil {
		logger.Errorf("Cannot clear user notifications: %s", err)
	}
	if msgText == "" {
		logger.Info("Message without text, skipping notification part")
		return
	}

	logger.WithField("message_text", msgText).Info("Searching for users mentioned in the message text")
	users, err := b.storage.FindUsersByLabel(ctx, msg.Chat.ID, msgText)
	if err != nil {
		logger.Errorf("Cannot get users from storage to notify: %s", err)
		return
	}
	logger.WithField("users", users).Info("Users mentioned in the message")
	if !conf.NotifyYourself {
		// for debug purposes
		users = excludeUserFromList(users, msg.From)
	}
	users = b.filterNotChatUsers(ctx, users, msg.Chat)
	b.notifyUsers(ctx, users, msg)
}

func (b *Bot) addChatMemberHandler(ctx context.Context, msg *models.Message) {
	chat := msg.Chat
	userID := msg.NewChatMember.ID
	if !b.createChat(ctx, chat) {
		return
	}

	b.addUserToChat(ctx, chat.ID, userID)
}

func (b *Bot) removeChatMemberHandler(ctx context.Context, msg *models.Message) {
	chat := msg.Chat
	userID := msg.LeftChatMember.ID
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithFields(log.Fields{"user_id": userID, "chat_id": chat.ID}).Info("Remove user-chat relation")
	err := b.storage.RemoveUserFromChat(ctx, chat.ID, userID)
	if err != nil {
		logger.Errorf("Cannot remove user-chat relation from the storage: %s", err)
		return
	}
}

func (b *Bot) createChatHandler(ctx context.Context, msg *models.Message) {
	b.createChat(ctx, msg.Chat)
}

func (b *Bot) deleteChatHandler(ctx context.Context, msg *models.Message) {
	chatID := msg.Chat.ID
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("chat_id", chatID).Info("Delete chat entry from the storage")
	err := b.storage.DeleteChat(ctx, chatID)
	if err != nil {
		logger.Errorf("Cannot remove chat from the storage: %s", err)
		return
	}
}

func (b *Bot) addUserLabelHandler(ctx context.Context, msg *models.Message) {
	user := msg.From
	_, label := msg.ToCommand()
	logger := logging.FromContextAndBase(ctx, gLogger)
	label = processText(label)

	if !b.createUser(ctx, user) {
		b.sendErrorMsg(ctx, user)
		return
	}

	if label == "" {
		b.sendMissLabelArgMsg(ctx, user, addLabelCmd)
		return
	}

	logger.WithFields(log.Fields{"label": label, "user_id": user.ID}).Info("Saving new user label in the storage")
	err := b.storage.AddLabelToUser(ctx, user.ID, label)
	if err != nil {
		logger.Errorf("Cannot save user label: %s", err)
		b.sendErrorMsg(ctx, user)
		return
	}

	b.sendOKMsg(ctx, user)
}

func (b *Bot) removeUserLabelHandler(ctx context.Context, msg *models.Message) {
	user := msg.From
	_, label := msg.ToCommand()
	logger := logging.FromContextAndBase(ctx, gLogger)
	label = processText(label)

	if !b.createUser(ctx, user) {
		b.sendErrorMsg(ctx, user)
		return
	}

	if label == "" {
		b.sendMissLabelArgMsg(ctx, user, removeLabelCmd)
		return
	}

	logger.WithFields(log.Fields{"label": label, "user_id": user.ID}).Info("Discarding user label from the storage")
	err := b.storage.RemoveLabelFromUser(ctx, user.ID, label)
	if err != nil {
		logger.Errorf("Removing user label from the storage failed: %s", err)
		b.sendErrorMsg(ctx, user)
		return
	}

	b.sendOKMsg(ctx, user)
}

func (b *Bot) showUserLabelsHandler(ctx context.Context, msg *models.Message) {
	user := msg.From
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

func excludeUserFromList(users []*models.User, userToExclude *models.User) []*models.User {
	var result []*models.User
	for _, u := range users {
		if u.ID == userToExclude.ID {
			continue
		}
		result = append(result, u)
	}
	return result
}

func processText(text string) string {
	return strings.ToLower(strings.TrimSpace(text))
}
