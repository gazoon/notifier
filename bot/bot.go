package bot

import (
	"context"
	"fmt"
	"notifier/config"
	"notifier/models"
	"notifier/storage"
	"strconv"
	"strings"
	"unicode/utf8"

	"notifier/notifications"
	"notifier/notifications_registry"

	log "github.com/Sirupsen/logrus"
	"github.com/gazoon/bot_libs/logging"
	"github.com/gazoon/bot_libs/messenger"
	"github.com/gazoon/bot_libs/queue/messages"
	"github.com/gazoon/bot_libs/speech"
	"github.com/pkg/errors"
)

const (
	addLabelCmd                = "addLabel"
	removeLabelCmd             = "removeLabel"
	showLabelsCmd              = "showLabels"
	setDelayCmd                = "notifDelay"
	mentioningMethodCmd        = "mentioningMethod"
	deleteSentNotificationsCmd = "deleteSentNotifications"

	setLangCmd          = "setLanguage"
	enableSwearWordCmd  = "addSwearWord"
	disableSwearWordCmd = "removeSwearWord"
	helpCmd             = "help"
)

var (
	gLogger = logging.WithPackage("bot")

	helpInPrivateChat = fmt.Sprintf("Hi! I can do following for you:\n\n"+
		"%s - Add new label for further notifications.\n\n"+
		"%s - Delete label with provided name.\n\n"+
		"%s - Show all your labels.\n\n"+
		"%s - Change the time, in seconds, after which I will send notification, default - 10 sec, 0 means notify immediately.\n\n"+
		"%s - Change the method, by which people can mention you, e.g: %s, %s, %s or %s.\n\n"+
		"%s - true or false - can I delete already sent notifications in case you showed some activity in the chat.\n\n"+
		"%s - language of voice messages. This command works only in group chats.\n"+
		"%s - add swear word to the bot vocabulary. This command works only in group chats.\n"+
		"%s - stop considering the word as swear. This command works only in group chats.",
		addLabelCmd, removeLabelCmd, showLabelsCmd, setDelayCmd, mentioningMethodCmd,
		models.TextMentioningMethod, models.VoiceMentioningMethod, models.AllMentioningMethod, models.NoneMentioningMethod,
		deleteSentNotificationsCmd, setLangCmd, enableSwearWordCmd, disableSwearWordCmd)
	helpInGroupChat = fmt.Sprintf("Hi! I can do following for you:\n\n"+
		"%s - change language of voice messages.\n\n"+
		"%s - add swear word to my vocabulary.\n\n"+
		"%s - stop considering the word as swear.",
		setLangCmd, enableSwearWordCmd, disableSwearWordCmd)
	chatGreetingTemplate = "Hi, %s! I will notify users in this chat if they was mentioned in the messages.\n" +
		"Your current language for voice messages is %s. You always can change it, to do this type:\n" +
		setLangCmd + " {desired_language}"

	notificationTextMentioningTemplate  = "You've been mentioned in the %s chat:"
	notificationVoiceMentioningTemplate = "You've been mentioned in the %s chat, %s:"
	noSwearWords                        = "✅ There are no swear words"
	swearWordFoundTemplate              = "❌ Swear words were detected -> %s"
	swearWordsDetectionFailed           = "Cannot detect the presence of swear words."
	errorText                           = "An internal bot error occurred."
	noLabelsText                        = "You don't have any labels yet."
	cmdArgMissedTextTemplate            = "You didn't provide a value for {%s} argument\nEnter %s {%s}"
	cmdBadArgTextTemplate               = "You provided a bad value for {%s} argument: %s."
	recognitionErrTemplate              = "Cannot recognize voice message request_id=%s"
	langUnsupportedTemplate             = "Language %s is unsupported. List of supported languages:\n" +
		strings.Join(models.SupportedLangsList, "\n")
	okText = "OK."
)

type Handler struct {
	Func func(ctx context.Context, msg *msgsqueue.Message)
	Name string
}

type Bot struct {
	notificationQueue     notifqueue.Producer
	notificationsRegistry notifregistry.ReadDeleter
	messenger             messenger.Messenger
	storage               storage.Storage
	recognizer            speech.Recognizer
	userCommands          map[string]*Handler
	chatCommands          map[string]*Handler
}

func New(notificationQueue notifqueue.Producer, notificationsRegistry notifregistry.ReadDeleter,
	messenger messenger.Messenger, storage storage.Storage, recognizer speech.Recognizer) *Bot {

	b := &Bot{
		notificationQueue:     notificationQueue,
		notificationsRegistry: notificationsRegistry,
		messenger:             messenger,
		storage:               storage,
		recognizer:            recognizer,
	}
	b.userCommands = b.createUserCommandsRegister()
	b.chatCommands = b.createChatCommandsRegister()
	return b
}

func fillUpCommandsRegister(register map[string]*Handler) {
	for cmdName, handler := range register {
		register[strings.ToLower(cmdName)] = handler
		register[strings.ToUpper(cmdName)] = handler
	}
}

func (b *Bot) createUserCommandsRegister() map[string]*Handler {
	register := map[string]*Handler{
		addLabelCmd:                {b.addUserLabelHandler, "addUserLabel"},
		removeLabelCmd:             {b.removeUserLabelHandler, "removeUserLabel"},
		showLabelsCmd:              {b.showUserLabelsHandler, "showUserLabels"},
		setDelayCmd:                {b.setDelayHandler, "setNotificationDelay"},
		mentioningMethodCmd:        {b.setMentioningMethodHandler, "setMentioningMethod"},
		deleteSentNotificationsCmd: {b.setCanDeleteNotificationsHandler, "setCanDeleteNotifications"},
	}
	fillUpCommandsRegister(register)
	return register
}

func (b *Bot) createChatCommandsRegister() map[string]*Handler {
	register := map[string]*Handler{
		setLangCmd:          {b.setChatLangHandler, "setChatLang"},
		enableSwearWordCmd:  {b.enableSwearWordHandler, "enableSwearWord"},
		disableSwearWordCmd: {b.disableSwearWordHandler, "disableSwearWord"},
		helpCmd:             {b.helpInChatHandler, "helpInGroupChat"},
	}
	fillUpCommandsRegister(register)
	return register
}

func (b *Bot) DispatchMessage(ctx context.Context, msg *msgsqueue.Message) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	if msg.MessageID == 0 {
		logger.Info("Cannot handle messages without message id, skip")
		return
	}
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
			cmd, _ := msg.ToCommand()
			var isCommand bool
			handler, isCommand = b.chatCommands[cmd]
			if !isCommand {
				handler = &Handler{b.regularMessageHandler, "regularMessage"}
			}
		}
	} else {
		cmd, _ := msg.ToCommand()
		var ok bool
		handler, ok = b.userCommands[cmd]
		if !ok {
			handler = &Handler{b.commandsListHandler, "commandsList"}
		}
	}
	logger.WithField("handler_name", handler.Name).Info("Calling handler")
	handler.Func(ctx, msg)
}

func (b *Bot) syncUserWithStorage(ctx context.Context, queueUser *msgsqueue.User, userChatID int) (*models.User, bool) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	user := &models.User{User: *queueUser}
	logger.WithField("user", user).Info("Saving user in the storage if doesn't exist")
	err := b.storage.GetOrCreateUser(ctx, user, userChatID)
	if err != nil {
		logger.Errorf("Cannot save user in the storage: %s", err)
		user.PMID = userChatID
		return user, false
	}
	return user, true
}

func (b *Bot) getUserFromStorage(ctx context.Context, userID int) *models.User {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("user_id", userID).Info("Get user from storage")
	user, err := b.storage.GetUser(ctx, userID)
	if err != nil {
		logger.Errorf("Storage doesn't return user data: %s", err)
		return nil
	}
	return user
}

func (b *Bot) syncChatWithStorage(ctx context.Context, queueChat *msgsqueue.Chat) (*models.Chat, bool) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	chat := &models.Chat{Chat: *queueChat}
	logger.WithField("chat", chat).Info("Saving chat in the storage")
	err := b.storage.GetOrCreateChat(ctx, chat)
	if err != nil {
		logger.Errorf("Cannot save chat in the storage: %s", err)
		return chat, false
	}
	return chat, true
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

func (b *Bot) notifyUsers(ctx context.Context, users []*models.User, msg *msgsqueue.Message, notificationText string) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	for _, user := range users {
		notification := models.NewNotification(user, msg.MessageID, msg.Chat.ID, notificationText,
			msg.RequestID)
		logger.WithField("notification", notification).Info("Put notification to the queue")
		err := b.notificationQueue.Put(ctx, notification)
		if err != nil {
			logger.Errorf("Cannot save user notification: %s", err)
			continue
		}
	}
}

func (b *Bot) sendErrorMsgToUser(ctx context.Context, user *models.User) {
	b.sendErrorMsg(ctx, user.PMID)
}

func (b *Bot) sendErrorMsg(ctx context.Context, chatID int) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("chat_id", chatID).Info("Sending error msg to the chat")
	_, err := b.messenger.SendText(ctx, chatID, errorText)
	if err != nil {
		logger.Errorf("Cannot send error msg: %s", err)
		return
	}
}

func (b *Bot) sendRecognitionErrorMsg(ctx context.Context, msg *msgsqueue.Message) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithFields(log.Fields{"chat_id": msg.Chat.ID, "msg_id": msg.MessageID}).Info("Sending recognition error to the chat")
	errorText := fmt.Sprintf(recognitionErrTemplate, msg.RequestID)
	_, err := b.messenger.SendReply(ctx, msg.Chat.ID, msg.MessageID, errorText)
	if err != nil {
		logger.Errorf("Cannot send recognition error msg: %s", err)
		return
	}
}

func (b *Bot) sendSwearDetectionReport(ctx context.Context, msg *msgsqueue.Message, result string) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithFields(log.Fields{"chat_id": msg.Chat.ID, "msg_id": msg.MessageID, "result": result}).
		Info("Sending swear report to the chat")
	_, err := b.messenger.SendReply(ctx, msg.Chat.ID, msg.MessageID, result)
	if err != nil {
		logger.Errorf("Cannot send swear report msg: %s", err)
		return
	}
}

func (b *Bot) sendOKMsgToUser(ctx context.Context, user *models.User) {
	b.sendOKMsg(ctx, user.PMID)
}

func (b *Bot) sendOKMsg(ctx context.Context, chatID int) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("chat_id", chatID).Info("Sending ok message")
	_, err := b.messenger.SendText(ctx, chatID, okText)
	if err != nil {
		logger.Errorf("Cannot send ok msg to the chat %s", err)
		return
	}
}

func (b *Bot) sendMissArgMsgToUser(ctx context.Context, user *models.User, cmd, argName string) {
	b.sendMissArgMsg(ctx, user.PMID, cmd, argName)
}

func (b *Bot) sendMissArgMsg(ctx context.Context, chatID int, cmd, argName string) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("chat_id", chatID).Infof("Call cmd %s without arg %s, sending arg missed message", cmd, argName)
	text := fmt.Sprintf(cmdArgMissedTextTemplate, argName, cmd, argName)
	_, err := b.messenger.SendText(ctx, chatID, text)
	if err != nil {
		logger.Errorf("Cannot send arg missed msg: %s", err)
		return
	}
}

func (b *Bot) sendBadArgMsgToUser(ctx context.Context, user *models.User, cmd, argName, argValue string, argErr error) {
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

func (b *Bot) sendUnsupportedLangMsg(ctx context.Context, chatID int, lang string) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("chat_id", chatID).Infof("Send lang is unsupported msg, lang: %s", lang)
	text := fmt.Sprintf(langUnsupportedTemplate, lang)
	_, err := b.messenger.SendText(ctx, chatID, text)
	if err != nil {
		logger.Errorf("Cannot send unsupported lang msg: %s", err)
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

func (b *Bot) commandsListHandler(ctx context.Context, msg *msgsqueue.Message) {
	logger := logging.FromContextAndBase(ctx, gLogger)

	user, _ := b.syncUserWithStorage(ctx, msg.From, msg.Chat.ID)

	logger.WithField("user_id", user.ID).Info("Sending the list of commands")
	_, err := b.messenger.SendText(ctx, user.PMID, helpInPrivateChat)
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

func (b *Bot) regularMessageHandler(ctx context.Context, msg *msgsqueue.Message) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	chat, ok := b.syncChatWithStorage(ctx, msg.Chat)
	if !ok {
		return
	}
	b.addUserToChat(ctx, chat.ID, msg.From.ID)

	sender := b.getUserFromStorage(ctx, msg.From.ID)
	if sender != nil {
		b.removeUserNotifications(ctx, sender, chat.ID)
	}
	var isVoiceMessage bool
	if msg.Text != "" {
		logger.Info("Processing text message in the chat")
		isVoiceMessage = false
	} else if msg.Voice != nil {
		logger.Info("Processing voice message in the chat")
		isVoiceMessage = true
	} else {
		logger.Info("Message without text or voice, skipping notification part")
		return
	}

	logger.WithField("chat_id", chat.ID).Info("Retrieving chat users")
	users, err := b.storage.GetChatUsers(ctx, chat.ID)
	if err != nil {
		logger.Errorf("Cannot get users from storage to notify: %s", err)
		return
	}
	logger.WithField("users", users).Info("Users in the chat")

	users = b.filterNotChatUsers(ctx, users, chat)
	if !config.GetInstance().NotifyYourself && sender != nil {
		// for debug purposes
		logger.WithField("yourself", sender).Info("Excluding yourself from the list of users to notify")
		users = models.ExcludeUserFromList(users, sender)
	}
	var wordsInMessage []string
	if isVoiceMessage {
		logger.WithField("voice", msg.Voice).Info("Voice mentioning, leave users who can be mentioned by voice")
		users = models.FilterByMentioningMethod(users, models.VoiceMentioningMethod)
		fileContent, err := b.messenger.DownloadFile(ctx, msg.Voice.ID)
		if err != nil {
			logger.Errorf("Messenger doesn't return file content for voice %s: %s", msg.Voice.ID, err)
			return
		}
		audioToRecognize := &speech.Audio{
			Content: fileContent, Encoding: msg.Voice.Encoding, SampleRate: msg.Voice.SampleRate}
		voiceLang := chat.Lang
		usersLabels := models.EnumLabels(users)
		enabledChatWords, err := b.storage.GetChatEnabledWords(ctx, chat.ID)
		if err != nil {
			logger.WithField("chat_id", chat.ID).Errorf("Cannot get list of swear words enabled in chat: %s", err)
			enabledChatWords = []string{}
		}
		disabledChatWords, err := b.storage.GetChatDisabledWords(ctx, chat.ID)
		if err != nil {
			logger.WithField("chat_id", chat.ID).Errorf("Cannot get list of chat swear words disabled in chat: %s", err)
			disabledChatWords = []string{}
		}
		logger.WithFields(log.Fields{
			"lang":                 voiceLang,
			"user_labels":          usersLabels,
			"enabled_swear_words":  enabledChatWords,
			"disabled_swear_words": disabledChatWords}).
			Info("Fetching words from voice message")
		wordsInMessage, err = b.recognizer.WordsFromAudio(ctx, audioToRecognize, voiceLang, usersLabels,
			enabledChatWords, disabledChatWords)
		if err != nil {
			logger.Warnf("Audio recognizer failed: %s", err)
			b.sendRecognitionErrorMsg(ctx, msg)
			return
		}
	} else {
		logger.WithField("text", msg.Text).Info("Text mentioning, leave users who can be mentioned by text")
		users = models.FilterByMentioningMethod(users, models.TextMentioningMethod)
		wordsInMessage = speech.UniqueWordsFromText(msg.Text)
	}
	wordsInMessage = models.ProcessWords(wordsInMessage)
	logger.WithField("words", wordsInMessage).Info("Words in message after processing")
	var notificationText string
	if isVoiceMessage {
		var swearDetectionReport string
		logger.Info("Detect are there any swear words in the message")
		swearWords, err := b.detectSwearWord(ctx, chat.ID, wordsInMessage)
		if err != nil {
			logger.Errorf("Cannot detect swear word: %s", err)
			swearDetectionReport = swearWordsDetectionFailed
		} else if swearWords == nil {
			logger.Info("No swear word was found")
			swearDetectionReport = noSwearWords
		} else {
			logger.WithField("swear_words", swearWords).Info("Swear word were detected")
			swearDetectionReport = fmt.Sprintf(swearWordFoundTemplate, strings.Join(swearWords, ", "))
		}
		notificationText = fmt.Sprintf(notificationVoiceMentioningTemplate, chat.Title, swearDetectionReport)
		b.sendSwearDetectionReport(ctx, msg, swearDetectionReport)
	} else {
		notificationText = fmt.Sprintf(notificationTextMentioningTemplate, chat.Title)
	}
	logger.WithField("users", users).Info("Search mentioned users to notify")
	users = models.GetMentionedUsers(users, wordsInMessage)
	logger.WithField("users", users).Info("Users to notify")
	b.notifyUsers(ctx, users, msg, notificationText)
}

func (b *Bot) detectSwearWord(ctx context.Context, chatID int, words []string) ([]string, error) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	wordsWithPrefixes := words
	prefixesToOriginal := map[string]string{}
	for _, word := range words {
		prefixesToOriginal[word] = word
		wordLen := utf8.RuneCountInString(word)
		wordRunes := []rune(word)
		for i := 1; i < wordLen; i++ {
			prefix := string(wordRunes[:wordLen-i])
			prefixesToOriginal[prefix] = word
			wordsWithPrefixes = append(wordsWithPrefixes, prefix)
		}
	}
	logger.WithField("words", wordsWithPrefixes).Info("Filter swear words for particular chat")
	swearWords, err := b.storage.FilterSwearWordsForChat(ctx, chatID, wordsWithPrefixes)
	if err != nil {
		return nil, errors.Wrap(err, "storage filter failed")
	}
	logger.WithField("swear_words", swearWords).Info("Swear words for chat")
	var result []string
	for _, word := range swearWords {
		originalWord, ok := prefixesToOriginal[word]
		if ok {
			result = append(result, originalWord)
		}
	}
	return result, nil
}

func (b *Bot) addChatMemberHandler(ctx context.Context, msg *msgsqueue.Message) {
	userID := msg.NewChatMember.ID
	chat, ok := b.syncChatWithStorage(ctx, msg.Chat)
	if !ok {
		return
	}

	b.addUserToChat(ctx, chat.ID, userID)
}

func (b *Bot) removeChatMemberHandler(ctx context.Context, msg *msgsqueue.Message) {
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

func (b *Bot) createChatHandler(ctx context.Context, msg *msgsqueue.Message) {
	chat, ok := b.syncChatWithStorage(ctx, msg.Chat)
	if !ok {
		b.sendErrorMsg(ctx, chat.ID)
		return
	}
	greetingText := fmt.Sprintf(chatGreetingTemplate, chat.Title, chat.Lang)
	_, err := b.messenger.SendText(ctx, chat.ID, greetingText)
	if err != nil {
		logger := logging.FromContextAndBase(ctx, gLogger)
		logger.WithField("chat_id", chat.ID).Errorf("Cannot send greeting text to the chat: %s", err)
	}
}

func (b *Bot) deleteChatHandler(ctx context.Context, msg *msgsqueue.Message) {
	chatID := msg.Chat.ID
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("chat_id", chatID).Info("Delete chat entry from the storage")
	err := b.storage.DeleteChat(ctx, chatID)
	if err != nil {
		logger.Errorf("Cannot remove chat from the storage: %s", err)
		return
	}
}

func (b *Bot) addUserLabelHandler(ctx context.Context, msg *msgsqueue.Message) {
	_, label := msg.ToCommand()
	logger := logging.FromContextAndBase(ctx, gLogger)
	label = models.ProcessWord(label)
	user, ok := b.syncUserWithStorage(ctx, msg.From, msg.Chat.ID)
	if !ok {
		b.sendErrorMsgToUser(ctx, user)
		return
	}

	if label == "" {
		b.sendMissArgMsgToUser(ctx, user, addLabelCmd, "new_label")
		return
	}

	logger.WithFields(log.Fields{"label": label, "user_id": user.ID}).Info("Saving new user label in the storage")
	err := b.storage.AddLabelToUser(ctx, user.ID, label)
	if err != nil {
		logger.Errorf("Cannot save user label: %s", err)
		b.sendErrorMsgToUser(ctx, user)
		return
	}

	b.sendOKMsgToUser(ctx, user)
}

func (b *Bot) removeUserLabelHandler(ctx context.Context, msg *msgsqueue.Message) {
	_, label := msg.ToCommand()
	logger := logging.FromContextAndBase(ctx, gLogger)
	label = models.ProcessWord(label)

	user, ok := b.syncUserWithStorage(ctx, msg.From, msg.Chat.ID)
	if !ok {
		b.sendErrorMsgToUser(ctx, user)
		return
	}

	if label == "" {
		b.sendMissArgMsgToUser(ctx, user, removeLabelCmd, "label_to_remove")
		return
	}

	logger.WithFields(log.Fields{"label": label, "user_id": user.ID}).Info("Discarding user label from the storage")
	err := b.storage.RemoveLabelFromUser(ctx, user.ID, label)
	if err != nil {
		logger.Errorf("Removing user label from the storage failed: %s", err)
		b.sendErrorMsgToUser(ctx, user)
		return
	}

	b.sendOKMsgToUser(ctx, user)
}

func (b *Bot) showUserLabelsHandler(ctx context.Context, msg *msgsqueue.Message) {
	user, ok := b.syncUserWithStorage(ctx, msg.From, msg.Chat.ID)
	if !ok {
		b.sendErrorMsgToUser(ctx, user)
		return
	}

	b.sendUserLabels(ctx, user)
}

func (b *Bot) setDelayHandler(ctx context.Context, msg *msgsqueue.Message) {
	logger := logging.FromContextAndBase(ctx, gLogger)

	user, ok := b.syncUserWithStorage(ctx, msg.From, msg.Chat.ID)
	if !ok {
		b.sendErrorMsgToUser(ctx, user)
		return
	}

	_, delayArg := msg.ToCommand()
	if delayArg == "" {
		b.sendMissArgMsgToUser(ctx, user, setDelayCmd, "notification_delay")
		return
	}
	delay, err := strconv.Atoi(delayArg)
	if err != nil {
		b.sendBadArgMsgToUser(ctx, user, setDelayCmd, "notification_delay", delayArg, err)
		return
	}

	logger.WithFields(log.Fields{"user_id": user.ID, "notification_delay": delay}).
		Info("Save custom notification delay value")
	err = b.storage.SetNotificationDelay(ctx, user.ID, delay)
	if err != nil {
		logger.Errorf("Cannot save user notification delay in the storage: %s", err)
		b.sendErrorMsgToUser(ctx, user)
		return
	}

	b.sendOKMsgToUser(ctx, user)
}

func (b *Bot) setMentioningMethodHandler(ctx context.Context, msg *msgsqueue.Message) {
	logger := logging.FromContextAndBase(ctx, gLogger)

	user, ok := b.syncUserWithStorage(ctx, msg.From, msg.Chat.ID)
	if !ok {
		b.sendErrorMsgToUser(ctx, user)
		return
	}

	_, mentioningMethod := msg.ToCommand()
	if mentioningMethod == "" {
		b.sendMissArgMsgToUser(ctx, user, mentioningMethodCmd, "mentioning_method")
		return
	}
	mentioningMethod = strings.ToLower(mentioningMethod)
	ok = models.IsValidMentioningMethod(mentioningMethod)
	if !ok {
		errInfo := errors.Errorf("valid values: %s", models.MentioningMethodsList)
		b.sendBadArgMsgToUser(ctx, user, mentioningMethodCmd, "mentioning_method", mentioningMethod, errInfo)
		return
	}

	logger.WithFields(log.Fields{"user_id": user.ID, "mentioning_method": mentioningMethod}).
		Info("Save custom mentioning method")
	err := b.storage.SetMentioningMethod(ctx, user.ID, mentioningMethod)
	if err != nil {
		logger.Errorf("Cannot save user mentioning setting in the storage: %s", err)
		b.sendErrorMsgToUser(ctx, user)
		return
	}

	b.sendOKMsgToUser(ctx, user)
}

func (b *Bot) setCanDeleteNotificationsHandler(ctx context.Context, msg *msgsqueue.Message) {
	logger := logging.FromContextAndBase(ctx, gLogger)

	user, ok := b.syncUserWithStorage(ctx, msg.From, msg.Chat.ID)
	if !ok {
		b.sendErrorMsgToUser(ctx, user)
		return
	}

	_, flagArgValue := msg.ToCommand()
	if flagArgValue == "" {
		b.sendMissArgMsgToUser(ctx, user, deleteSentNotificationsCmd, "can_or_not")
		return
	}
	canDelete, err := strconv.ParseBool(flagArgValue)
	if err != nil {
		b.sendBadArgMsgToUser(ctx, user, deleteSentNotificationsCmd, "can_or_not", flagArgValue, err)
		return
	}

	logger.WithFields(log.Fields{"user": user, "can_delete_notifications": canDelete}).
		Info("Set custom value for the CanDeleteNotifications flag")

	if user.CanDeleteNotifications == canDelete {
		logger.Info("User already have the same value no need to update")
		b.sendOKMsgToUser(ctx, user)
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
		b.sendErrorMsgToUser(ctx, user)
		return
	}

	b.sendOKMsgToUser(ctx, user)
}

func (b *Bot) setChatLangHandler(ctx context.Context, msg *msgsqueue.Message) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	chat := msg.Chat
	_, lang := msg.ToCommand()
	if lang == "" {
		b.sendMissArgMsg(ctx, chat.ID, setLangCmd, "desired_lang")
		return
	}
	lang = models.LangToSupportedFormat(lang)
	ok := models.IsSupportedLang(lang)
	if !ok {
		b.sendUnsupportedLangMsg(ctx, chat.ID, lang)
		return
	}

	logger.WithFields(log.Fields{"chat_id": chat.ID, "lang": lang}).Info("Save custom language for the chat")
	err := b.storage.SetChatLang(ctx, chat.ID, lang)
	if err != nil {
		logger.Errorf("Cannot save chat language in the storage: %s", err)
		b.sendErrorMsg(ctx, chat.ID)
		return
	}

	b.sendOKMsg(ctx, chat.ID)
}

func (b *Bot) enableSwearWordHandler(ctx context.Context, msg *msgsqueue.Message) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	chat := msg.Chat
	_, word := msg.ToCommand()
	if word == "" {
		b.sendMissArgMsg(ctx, chat.ID, enableSwearWordCmd, "swear_word")
		return
	}
	word = models.ProcessWord(word)

	logger.WithFields(log.Fields{"chat_id": chat.ID, "swear_word": word}).Info("Save chat - swear word relation in the storage")
	err := b.storage.EnableChatWord(ctx, chat.ID, word)
	if err != nil {
		logger.Errorf("Cannot enable word for chat in storage: %s", err)
		b.sendErrorMsg(ctx, chat.ID)
		return
	}

	b.sendOKMsg(ctx, chat.ID)
}

func (b *Bot) disableSwearWordHandler(ctx context.Context, msg *msgsqueue.Message) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	chat := msg.Chat
	_, word := msg.ToCommand()
	if word == "" {
		b.sendMissArgMsg(ctx, chat.ID, disableSwearWordCmd, "swear_word")
		return
	}
	word = models.ProcessWord(word)

	logger.WithFields(log.Fields{"chat_id": chat.ID, "swear_word": word}).Info("Remove chat - swear word relation in the storage")
	err := b.storage.DisableChatWord(ctx, chat.ID, word)
	if err != nil {
		logger.Errorf("Cannot disable word for chat in storage: %s", err)
		b.sendErrorMsg(ctx, chat.ID)
		return
	}

	b.sendOKMsg(ctx, chat.ID)
}

func (b *Bot) helpInChatHandler(ctx context.Context, msg *msgsqueue.Message) {
	logger := logging.FromContextAndBase(ctx, gLogger)

	logger.WithField("chat_id", msg.Chat.ID).Info("Sending help message to chat")
	_, err := b.messenger.SendText(ctx, msg.Chat.ID, helpInGroupChat)
	if err != nil {
		logger.Errorf("cannot send help to chat: %s", err)
	}
}
