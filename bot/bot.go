package bot

import (
	"context"
	"fmt"
	"notifier/config"
	"notifier/libs/logging"
	"notifier/libs/messenger"
	"notifier/libs/models"
	"notifier/libs/queue/messages"
	"notifier/libs/queue/notifications"
	"notifier/storage"
	"strconv"
	"strings"
	"sync"

	"notifier/libs/speech"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"notifier/libs/notifications_registry"
)

const (
	addLabelCmd                = "addLabel"
	removeLabelCmd             = "removeLabel"
	showLabelsCmd              = "showLabels"
	setDelayCmd                = "notifDelay"
	mentioningMethodCmd        = "mentioningMethod"
	deleteSentNotificationsCmd = "deleteSentNotifications"

	defaultVoiceLang = "ru-RU"
)

var (
	supportedLangs = [...]string{
		"af-ZA",
		"id-ID",
		"ms-MY",
		"ca-ES",
		"cs-CZ",
		"da-DK",
		"de-DE",
		"en-AU",
		"en-CA",
		"en-GB",
		"en-IN",
		"en-IE",
		"en-NZ",
		"en-PH",
		"en-ZA",
		"en-US",
		"es-AR",
		"es-BO",
		"es-CL",
		"es-CO",
		"es-CR",
		"es-EC",
		"es-SV",
		"es-ES",
		"es-US",
		"es-GT",
		"es-HN",
		"es-MX",
		"es-NI",
		"es-PA",
		"es-PY",
		"es-PE",
		"es-PR",
		"es-DO",
		"es-UY",
		"es-VE",
		"eu-ES",
		"fil-PH",
		"fr-CA",
		"fr-FR",
		"gl-ES",
		"hr-HR",
		"zu-ZA",
		"is-IS",
		"it-IT",
		"lt-LT",
		"hu-HU",
		"nl-NL",
		"nb-NO",
		"pl-PL",
		"pt-BR",
		"pt-PT",
		"ro-RO",
		"sk-SK",
		"sl-SI",
		"fi-FI",
		"sv-SE",
		"vi-VN",
		"tr-TR",
		"el-GR",
		"bg-BG",
		"ru-RU",
		"sr-RS",
		"uk-UA",
		"he-IL",
		"ar-IL",
		"ar-JO",
		"ar-AE",
		"ar-BH",
		"ar-DZ",
		"ar-SA",
		"ar-IQ",
		"ar-KW",
		"ar-MA",
		"ar-TN",
		"ar-OM",
		"ar-PS",
		"ar-QA",
		"ar-LB",
		"ar-EG",
		"fa-IR",
		"hi-IN",
		"th-TH",
		"ko-KR",
		"cmn-Hant-TW",
		"yue-Hant-HK",
		"ja-JP",
		"cmn-Hans-HK",
		"cmn-Hans-CN",
	}
)

var (
	gLogger = logging.WithPackage("bot")

	commandsText = fmt.Sprintf("Hi! I can do following for you:\n%s - Add new label for further notifications.\n"+
		"%s - Delete label with provided name.\n%s - Show all your labels.\n"+
		"%s - Change the time, in seconds, after which I will send notification, default - 10 sec, 0 means notify immediately.\n"+
		"%s - Change the method, by which people can mention you, e.g: %s, %s, %s or %s.\n"+
		"%s - true or false - can I delete already sent notifications in case you showed some activity in the chat.",
		addLabelCmd, removeLabelCmd, showLabelsCmd, setDelayCmd, mentioningMethodCmd,
		models.TextMentioningMethod, models.VoiceMentioningMethod, models.AllMentioningMethod, models.NoneMentioningMethod,
		deleteSentNotificationsCmd)

	notificationTextTemplate = "%s, you've been mentioned in the %s chat:"
	errorText                = "An internal bot error occurred."
	noLabelsText             = "You don't have any labels yet."
	cmdArgMissedTextTemplate = "You didn't provide a value for {%s} argument\nEnter %s {%s}"
	cmdBadArgTextTemplate    = "You provided a bad value for {%s} argument: %s."
	recognitionErrTemplate   = "Cannot recognize voice message request_id=%s"
	okText                   = "OK."
)

func prepareContext(msg *models.Message) context.Context {
	ctx := context.Background()
	requestID := msg.RequestID
	logger := logging.WithRequestID(requestID)
	ctx = logging.NewContext(ctx, logger)
	return ctx
}

type Handler struct {
	Func func(ctx context.Context, msg *models.Message)
	Name string
}

type Bot struct {
	messagesQueue         msgsqueue.Consumer
	notificationQueue     notifqueue.Producer
	notificationsRegistry notifregistry.ReadDeleter
	messenger             messenger.Messenger
	storage               storage.Storage
	recognizer            speech.Recognizer
	commandsRegister      map[string]*Handler
	wg                    sync.WaitGroup
}

func New(messagesQueue msgsqueue.Consumer, notificationQueue notifqueue.Producer, notificationsRegistry notifregistry.ReadDeleter,
	messenger messenger.Messenger, storage storage.Storage, recognizer speech.Recognizer) *Bot {

	b := &Bot{
		messagesQueue:         messagesQueue,
		notificationQueue:     notificationQueue,
		notificationsRegistry: notificationsRegistry,
		messenger:             messenger,
		storage:               storage,
		recognizer:            recognizer,
	}
	b.commandsRegister = b.createCommandRegister()
	return b
}

func (b *Bot) createCommandRegister() map[string]*Handler {
	register := map[string]*Handler{
		addLabelCmd:                {b.addUserLabelHandler, "addUserLabel"},
		removeLabelCmd:             {b.removeUserLabelHandler, "removeUserLabel"},
		showLabelsCmd:              {b.showUserLabelsHandler, "showUserLabels"},
		setDelayCmd:                {b.setDelayHandler, "setNotificationDelay"},
		mentioningMethodCmd:        {b.setMentioningMethodHandler, "setMentioningMethod"},
		deleteSentNotificationsCmd: {b.setCanDeleteNotificationsHandler, "setCanDeleteNotifications"},
	}
	for cmdName, handler := range register {
		register[strings.ToLower(cmdName)] = handler
		register[strings.ToUpper(cmdName)] = handler
	}
	return register
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
				msg, processingID, ok := b.messagesQueue.GetNext()
				if !ok {
					return
				}
				b.onMessage(msg, processingID)
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

func (b *Bot) onMessage(msg *models.Message, processingID string) {
	ctx := prepareContext(msg)
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("msg", msg).Info("Message received from incoming queue")
	b.dispatchMessage(ctx, msg)
	logger.WithField("processing_id", processingID).Info("Finish processing incoming message")
	b.messagesQueue.FinishProcessing(ctx, processingID)
}

func (b *Bot) dispatchMessage(ctx context.Context, msg *models.Message) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	var handler *Handler
	if !msg.Chat.IsPrivate {
		switch {
		case msg.IsBotAdded:
			handler = &Handler{b.createChatHandler, "createChat"}
		case msg.NewChatMember != nil:
			handler = &Handler{b.addChatMemberHandler, "addChatMember"}
		case msg.IsBotLeft:
			handler = &Handler{b.deleteChatHandler, "deleteChat"}
		case msg.LeftChatMember != nil:
			handler = &Handler{b.removeChatMemberHandler, "removeChatMember"}
		default:
			handler = &Handler{b.regularMessageHandler, "regularMessage"}
		}
	} else {
		cmd, _ := msg.ToCommand()
		var ok bool
		handler, ok = b.commandsRegister[cmd]
		if !ok {
			handler = &Handler{b.commandsListHandler, "commandsList"}
		}
	}
	logger.WithField("handler_name", handler.Name).Info("Calling handler")
	handler.Func(ctx, msg)
}

func (b *Bot) syncUserWithStorage(ctx context.Context, user *models.User, userChatID int) bool {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("user", user).Info("Saving user in the storage if doesn't exist")
	err := b.storage.GetOrCreateUser(ctx, user, userChatID)
	if err != nil {
		logger.Errorf("Cannot save user in the storage: %s", err)
		return false
	}
	return true
}

func (b *Bot) updateUserFromStorage(ctx context.Context, user *models.User) bool {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("user", user).Info("Get user from storage")
	isExists, err := b.storage.GetUser(ctx, user)
	if err != nil {
		logger.Errorf("Storage doesn't return user data: %s", err)
		return false
	}
	return isExists
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
		notification := models.NewNotification(user, msg.ID, msg.Chat.ID, notificationText,
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
	_, err := b.messenger.SendText(ctx, user.PMID, errorText)
	if err != nil {
		logger.Errorf("Cannot send error msg: %s", err)
		return
	}
}

func (b *Bot) sendRecognitionErrorMsg(ctx context.Context, msg *models.Message) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithFields(log.Fields{"chat_id": msg.Chat.ID, "msg_id": msg.ID}).Info("Sending recognition error to the chat")
	errorText := fmt.Sprintf(recognitionErrTemplate, msg.RequestID)
	_, err := b.messenger.SendReply(ctx, msg.Chat.ID, msg.ID, errorText)
	if err != nil {
		logger.Errorf("Cannot send recognition error msg: %s", err)
		return
	}
}

func (b *Bot) sendOKMsg(ctx context.Context, user *models.User) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("user", user).Info("Sending ok message")
	_, err := b.messenger.SendText(ctx, user.PMID, okText)
	if err != nil {
		logger.Errorf("Cannot send ok msg to the user %s", err)
		return
	}
}

func (b *Bot) sendMissArgMsg(ctx context.Context, user *models.User, cmd, argName string) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("user", user).Infof("Call cmd %s without arg %s, sending arg missed message", cmd, argName)
	text := fmt.Sprintf(cmdArgMissedTextTemplate, argName, cmd, argName)
	_, err := b.messenger.SendText(ctx, user.PMID, text)
	if err != nil {
		logger.Errorf("Cannot send arg missed msg: %s", err)
		return
	}
}

func (b *Bot) sendBadArgMsg(ctx context.Context, user *models.User, cmd, argName, argValue string, argErr error) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("user", user).Infof("Call cmd %s arg %s with incorrect value=%s: %s, sending bad arg message",
		cmd, argName, argValue, argErr)
	text := fmt.Sprintf(cmdBadArgTextTemplate, argName, argErr)
	_, err := b.messenger.SendText(ctx, user.PMID, text)
	if err != nil {
		logger.Errorf("Cannot send bad arg msg: %s", err)
		return
	}
}

func (b *Bot) sendUserLabels(ctx context.Context, user *models.User) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	var labelsText string
	if len(user.Labels) != 0 {
		labelsText = strings.Join(user.Labels, "\n")
	} else {
		labelsText = noLabelsText
	}
	logger.WithField("labels_text", labelsText).Info("Sending labels to the user")
	_, err := b.messenger.SendText(ctx, user.PMID, labelsText)
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

	b.syncUserWithStorage(ctx, user, msg.Chat.ID)

	logger.WithField("user_id", user.ID).Info("Sending the list of commands")
	_, err := b.messenger.SendText(ctx, user.PMID, commandsText)
	if err != nil {
		logger.Errorf("cannot send commands to the user: %s", err)
	}
}

func (b *Bot) removeUserNotifications(ctx context.Context, user *models.User, chatID int) {
	logger := logging.FromContextAndBase(ctx, gLogger).WithFields(log.Fields{"user": user, "chat_id": chatID})
	logger.Info("Discarding not actual user notifications from the queue")
	err := b.notificationQueue.Discard(ctx, user, chatID)
	if err != nil {
		logger.Errorf("Cannot clear user notifications in the queue: %s", err)
	}
	if !user.CanDeleteNotifications {
		logger.Info("User preferred not to delete already sent notifications")
		return
	}
	logger.Info("Get already sent notifications to delete")
	sentNotifications, err := b.notificationsRegistry.Get(ctx, user.ID, chatID)
	if err != nil {
		logger.Errorf("Cannot retrieve sent notifications from the registry: %s", err)
		return
	}
	for _, sentMessage := range sentNotifications {
		logger = logger.WithField("sent_notification", sentMessage)
		logger.Info("Delete notification message from private chat with the user")
		err = b.messenger.DeleteMessage(ctx, sentMessage.MessageID, user.PMID)
		if err != nil {
			logger.Errorf("Cannot delete notification message from the messenger: %s", err)
		}
		logger.Info("Delete sent message from registry")
		err = b.notificationsRegistry.Delete(ctx, sentMessage)
		if err != nil {
			logger.Errorf("Cannot delete sent message from the registry: %s", err)
		}
	}

}

func (b *Bot) regularMessageHandler(ctx context.Context, msg *models.Message) {
	conf := config.GetInstance()
	logger := logging.FromContextAndBase(ctx, gLogger)

	if !b.createChat(ctx, msg.Chat) {
		return
	}
	sender := msg.From
	b.addUserToChat(ctx, msg.Chat.ID, sender.ID)

	isExists := b.updateUserFromStorage(ctx, sender)
	if isExists {
		b.removeUserNotifications(ctx, sender, msg.Chat.ID)
	}

	if msg.Text == "" && msg.Voice == nil {
		logger.Info("Message without text or voice, skipping notification part")
		return
	}

	logger.WithField("chat_id", msg.Chat.ID).Info("Retrieving chat users")
	users, err := b.storage.GetChatUsers(ctx, msg.Chat.ID)
	if err != nil {
		logger.Errorf("Cannot get users from storage to notify: %s", err)
		return
	}
	logger.WithField("users", users).Info("Users in the chat")

	users = b.filterNotChatUsers(ctx, users, msg.Chat)
	if !conf.NotifyYourself {
		// for debug purposes
		logger.WithField("yourself", sender).Info("Excluding yourself from the list of users to notify")
		users = models.ExcludeUserFromList(users, sender)
	}
	var wordsInMessage []string
	if msg.Text != "" {
		logger.WithField("text", msg.Text).Info("Text mentioning, leave users who can be mentioned by text")
		users = models.FilterByMentioningMethod(users, models.TextMentioningMethod)
		wordsInMessage = speech.UniqueWordsFromText(msg.Text)

	} else {
		logger.WithField("voice", msg.Voice).Info("Voice mentioning, leave users who can be mentioned by voice")
		users = models.FilterByMentioningMethod(users, models.VoiceMentioningMethod)
		fileContent, err := b.messenger.DownloadFile(ctx, msg.Voice.ID)
		if err != nil {
			logger.Errorf("Messenger doesn't return file content for voice %s: %s", msg.Voice.ID, err)
			return
		}
		audioToRecognize := &speech.Audio{
			Content: fileContent, Encoding: msg.Voice.Encoding, SampleRate: msg.Voice.SampleRate}
		voiceLang := defaultVoiceLang
		usersLabels := models.EnumLabels(users)
		logger.WithFields(log.Fields{"lang": voiceLang, "hints": usersLabels}).Info("Fetching words from voice message")
		wordsInMessage, err = b.recognizer.WordsFromAudio(ctx, audioToRecognize, defaultVoiceLang, usersLabels)
		if err != nil {
			logger.Warnf("Audio recognizer failed: %s", err)
			b.sendRecognitionErrorMsg(ctx, msg)
			return
		}
	}
	wordsInMessage = models.ProcessWords(wordsInMessage)
	logger.WithFields(log.Fields{"users": users, "words": wordsInMessage}).Info("Search mentioned users to notify")
	users = models.GetMentionedUsers(users, wordsInMessage)

	logger.WithField("users", users).Info("Users to notify")
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
	label = models.ProcessWord(label)

	if !b.syncUserWithStorage(ctx, user, msg.Chat.ID) {
		b.sendErrorMsg(ctx, user)
		return
	}

	if label == "" {
		b.sendMissArgMsg(ctx, user, addLabelCmd, "new_label")
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
	label = models.ProcessWord(label)

	if !b.syncUserWithStorage(ctx, user, msg.Chat.ID) {
		b.sendErrorMsg(ctx, user)
		return
	}

	if label == "" {
		b.sendMissArgMsg(ctx, user, removeLabelCmd, "label_to_remove")
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

	if !b.syncUserWithStorage(ctx, user, msg.Chat.ID) {
		b.sendErrorMsg(ctx, user)
		return
	}

	b.sendUserLabels(ctx, user)
}

func (b *Bot) setDelayHandler(ctx context.Context, msg *models.Message) {
	user := msg.From
	logger := logging.FromContextAndBase(ctx, gLogger)

	if !b.syncUserWithStorage(ctx, user, msg.Chat.ID) {
		b.sendErrorMsg(ctx, user)
		return
	}

	_, delayArg := msg.ToCommand()
	if delayArg == "" {
		b.sendMissArgMsg(ctx, user, setDelayCmd, "notification_delay")
		return
	}
	delay, err := strconv.Atoi(delayArg)
	if err != nil {
		b.sendBadArgMsg(ctx, user, setDelayCmd, "notification_delay", delayArg, err)
		return
	}

	logger.WithFields(log.Fields{"user_id": user.ID, "notification_delay": delay}).
		Info("Save custom notification delay value")
	err = b.storage.SetNotificationDelay(ctx, user.ID, delay)
	if err != nil {
		logger.Errorf("Cannot save user notification delay in the storage: %s", err)
		b.sendErrorMsg(ctx, user)
		return
	}

	b.sendOKMsg(ctx, user)
}

func (b *Bot) setMentioningMethodHandler(ctx context.Context, msg *models.Message) {
	user := msg.From
	logger := logging.FromContextAndBase(ctx, gLogger)

	if !b.syncUserWithStorage(ctx, user, msg.Chat.ID) {
		b.sendErrorMsg(ctx, user)
		return
	}

	_, mentioningMethod := msg.ToCommand()
	if mentioningMethod == "" {
		b.sendMissArgMsg(ctx, user, mentioningMethodCmd, "mentioning_method")
		return
	}
	mentioningMethod = strings.ToLower(mentioningMethod)
	ok := models.IsValidMentioningMethod(mentioningMethod)
	if !ok {
		errInfo := errors.Errorf("valid values: %s", models.MentioningMethodsList)
		b.sendBadArgMsg(ctx, user, mentioningMethodCmd, "mentioning_method", mentioningMethod, errInfo)
		return
	}

	logger.WithFields(log.Fields{"user_id": user.ID, "mentioning_method": mentioningMethod}).
		Info("Save custom mentioning method")
	err := b.storage.SetMentioningMethod(ctx, user.ID, mentioningMethod)
	if err != nil {
		logger.Errorf("Cannot save user mentioning setting in the storage: %s", err)
		b.sendErrorMsg(ctx, user)
		return
	}

	b.sendOKMsg(ctx, user)
}

func (b *Bot) setCanDeleteNotificationsHandler(ctx context.Context, msg *models.Message) {
	user := msg.From
	logger := logging.FromContextAndBase(ctx, gLogger)

	if !b.syncUserWithStorage(ctx, user, msg.Chat.ID) {
		b.sendErrorMsg(ctx, user)
		return
	}

	_, flagArgValue := msg.ToCommand()
	if flagArgValue == "" {
		b.sendMissArgMsg(ctx, user, deleteSentNotificationsCmd, "can_or_not")
		return
	}
	canDelete, err := strconv.ParseBool(flagArgValue)
	if err != nil {
		b.sendBadArgMsg(ctx, user, deleteSentNotificationsCmd, "can_or_not", flagArgValue, err)
		return
	}

	logger.WithFields(log.Fields{"user": user, "can_delete_notifications": canDelete}).
		Info("Set custom value for the CanDeleteNotifications flag")

	if user.CanDeleteNotifications == canDelete {
		logger.Info("User already have the save value no need to update")
		b.sendOKMsg(ctx, user)
		return
	}

	if canDelete {
		logger.Info("User want to set flag to true, delete all previous notifications from the registry")
		err = b.notificationsRegistry.DeleteAllForUser(ctx, user.ID)
		if err != nil {
			logger.Errorf("Notifications registry falied to delete all user notifications: %s", err)
		}
	}

	logger.Infof("Set CanDeleteNotifications to %v in the storage", canDelete)
	err = b.storage.SetCanDeleteNotifications(ctx, user.ID, canDelete)
	if err != nil {
		logger.Errorf("Cannot save user CanDeleteNotifications setting in the storage: %s", err)
		b.sendErrorMsg(ctx, user)
		return
	}

	b.sendOKMsg(ctx, user)
}
