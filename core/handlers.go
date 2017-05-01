package core

import (
	"context"
	"notifier/config"
	"notifier/models"
)

func dispatchMessage(ctx context.Context, msg *models.Message) {
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
