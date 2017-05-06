package core

import (
	"context"
	"notifier/config"
	"notifier/logging"
	"notifier/models"
	"notifier/storage"
	"strings"
)

var (
	gLogger = logging.WithPackage("core")
)

func dispatchMessage(ctx context.Context, msg *models.Message) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("receved_msg", msg).Info("Dispatching a message")
	if !msg.Chat.IsPrivate {
		if msg.NewChatMember != nil {
			if msg.NewChatMember.Username == config.Telegram.BotName {
				createChat(ctx, msg.Chat)
				return
			}
			addChatMember(ctx, msg.Chat, msg.NewChatMember.ID)
			return
		}

		if msg.LeftChatMember != nil {
			if msg.LeftChatMember.Username == config.Telegram.BotName {
				deleteChat(ctx, msg.Chat.ID)
				return
			}
			removeChatMember(ctx, msg.Chat, msg.LeftChatMember.ID)
			return
		}

		if msg.Text != "" {
			regularMessage(ctx, msg)
			return
		}
		return
	}
	cmd, label := msg.ToCommand()
	switch cmd {
	case addLabelCmd:
		addUserLabel(ctx, msg.From, label)
	case removeLabelCmd:
		removeUserLabel(ctx, msg.From, label)
	case showLabelsCmd:
		showUserLabels(ctx, msg.From)
	default:
		commandsList(ctx, msg.From)
	}
}

func commandsList(ctx context.Context, user *models.User) {
	logger := logging.FromContextAndBase(ctx, gLogger)

	err := storage.CreateUser(ctx, user)
	if err != nil {
		logger.Error(err)
		return
	}

	logger.Info(CommandsText)
}

func regularMessage(ctx context.Context, msg *models.Message) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	msgText := processText(msg.Text)

	err := storage.CreateChat(ctx, msg.Chat)
	if err != nil {
		logger.Error(err)
		return
	}

	err = storage.AddUserToChat(ctx, msg.Chat.ID, msg.From.ID)
	if err != nil {
		logger.Error(err)
		return
	}

	users, err := storage.FindUsersByLabel(ctx, msg.Chat.ID, msgText)
	if err != nil {
		logger.Error(err)
		return
	}

	logger.Info(users)
}

func addChatMember(ctx context.Context, chat *models.Chat, userID int) {
	logger := logging.FromContextAndBase(ctx, gLogger)

	err := storage.CreateChat(ctx, chat)
	if err != nil {
		logger.Errorf("Cannot initialize chat: %s", err)
		return
	}

	err = storage.AddUserToChat(ctx, chat.ID, userID)
	if err != nil {
		logger.Errorf("Cannot add user chat: %s", err)
		return
	}
}

func removeChatMember(ctx context.Context, chat *models.Chat, userID int) {
	logger := logging.FromContextAndBase(ctx, gLogger)

	err := storage.RemoveUserFromChat(ctx, chat.ID, userID)
	if err != nil {
		logger.Error(err)
		return
	}
}

func createChat(ctx context.Context, chat *models.Chat) {
	logger := logging.FromContextAndBase(ctx, gLogger)

	err := storage.CreateChat(ctx, chat)
	if err != nil {
		logger.Error(err)
		return
	}
}

func deleteChat(ctx context.Context, chatID int) {
	logger := logging.FromContextAndBase(ctx, gLogger)

	err := storage.DeleteChat(ctx, chatID)
	if err != nil {
		logger.Error(err)
		return
	}
}

func addUserLabel(ctx context.Context, user *models.User, label string) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	label = processText(label)

	err := storage.CreateUser(ctx, user)
	if err != nil {
		logger.Error(err)
		return
	}

	err = storage.AddLabelToUser(ctx, user.ID, label)
	if err != nil {
		logger.Error(err)
		return
	}
}

func removeUserLabel(ctx context.Context, user *models.User, label string) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	label = processText(label)

	err := storage.CreateUser(ctx, user)
	if err != nil {
		logger.Error(err)
		return
	}

	err = storage.RemoveLabelFromUser(ctx, user.ID, label)
	if err != nil {
		logger.Error(err)
		return
	}
}

func showUserLabels(ctx context.Context, user *models.User) {
	logger := logging.FromContextAndBase(ctx, gLogger)

	err := storage.CreateUser(ctx, user)
	if err != nil {
		logger.Error(err)
		return
	}

	labels, err := storage.GetUserLabels(ctx, user.ID)
	if err != nil {
		logger.Error(err)
		return
	}

	logger.Info(labels)
}

func processText(text string) string {
	return strings.ToLower(strings.TrimSpace(text))
}
