package models

import (
	"notifier/logging"
	"strings"
	"time"
)

type Message struct {
	ID             int       `bson:"message_id"`
	RequestID      string    `bson:"request_id"`
	Chat           *Chat     `bson:"chat"`
	Text           string    `bson:"text"`
	From           *User     `bson:"from"`
	NewChatMember  *User     `bson:"new_chat_member"`
	LeftChatMember *User     `bson:"left_chat_member"`
	IsBotLeft      bool      `bson:"is_bot_left"`
	IsBotAdded     bool      `bson:"is_bot_added"`
	CreatedAt      time.Time `bson:"created_at"`
}

func (m Message) String() string {
	return logging.ObjToString(&m)
}

func (m *Message) ToCommand() (string, string) {
	text := strings.TrimSpace(m.Text)
	parts := strings.SplitN(text, " ", 2)
	cmd := strings.ToLower(strings.TrimSpace(parts[0]))
	args := ""
	if len(parts) > 1 {
		args = strings.TrimSpace(parts[1])
	}
	return cmd, args
}

type Chat struct {
	ID        int    `bson:"id"`
	IsPrivate bool   `bson:"is_private"`
	Title     string `bson:"title"`
}

func (c Chat) String() string {
	return logging.ObjToString(&c)
}

type User struct {
	ID int `bson:"id"`
	//id of the private chat with the user
	PMID              int    `bson:"pmid"`
	Username          string `bson:"username"`
	Name              string `bson:"name"`
	NotificationDelay int    `bson:"-"`
}

func (u User) String() string {
	return logging.ObjToString(&u)
}

type Notification struct {
	RequestID string    `bson:"request_id"`
	Text      string    `bson:"text"`
	ReadyAt   time.Time `bson:"ready_at"`
	User      *User     `bson:"user"`
	MessageID int       `bson:"message_id"`
	ChatID    int       `bson:"chat_id"`
}

func NewNotification(user *User, msgID, chatID int, text, requestID string) *Notification {
	return &Notification{
		RequestID: requestID,
		Text:      text,
		ReadyAt:   time.Now().Add(time.Second * time.Duration(user.NotificationDelay)),
		User:      user,
		MessageID: msgID,
		ChatID:    chatID,
	}
}

func (n Notification) String() string {
	return logging.ObjToString(&n)
}
