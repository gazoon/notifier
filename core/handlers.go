package core

import (
	"context"
	"notifier/config"
	"notifier/logging"
	"notifier/models"
	"notifier/storage"
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

}

func regularMessage(ctx context.Context, msg *models.Message) {

}

func addChatMember(ctx context.Context, chat *models.Chat, userID int) {
	logger := logging.FromContextAndBase(ctx, gLogger)

	err := storage.GetOrCreateChat(ctx, chat)
	if err != nil {
		logger.Errorf("Cannot initialize chat: %s", err)
		return
	}

	err=storage.AddUserToChat(ctx,chat.ID,userID)
	if err != nil {
		logger.Errorf("Cannot add user chat: %s", err)
		return
	}
}

func removeChatMember(ctx context.Context, chat *models.Chat, userID int) {

}

func createChat(ctx context.Context, chat *models.Chat) {

}

func deleteChat(ctx context.Context, chatID int) {

}

func addUserLabel(ctx context.Context, user *models.User, label string) {

}

func removeUserLabel(ctx context.Context, user *models.User, label string) {

}

func showUserLabels(ctx context.Context, user *models.User) {

}

func createUser(ctx context.Context, user *models.User) {

}
