package models

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type Message struct {
	ID             int       `bson:"message_id"`
	RequestID      string    `bson:"request_id"`
	Chat           *Chat     `bson:"chat"`
	Text           string    `bson:"text"`
	Voice          *Voice    `bson:"voice"`
	From           *User     `bson:"from"`
	NewChatMember  *User     `bson:"new_chat_member"`
	LeftChatMember *User     `bson:"left_chat_member"`
	IsBotLeft      bool      `bson:"is_bot_left"`
	IsBotAdded     bool      `bson:"is_bot_added"`
	CreatedAt      time.Time `bson:"created_at"`
}

func (m Message) String() string {
	return ObjToString(&m)
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

	Lang      string `bson:"-"`
}

func (c Chat) String() string {
	return ObjToString(&c)
}

type Voice struct {
	ID       string `bson:"id"`
	Duration int    `bson:"duration"`
	Size     *int   `bson:"size"`
}

func (v Voice) String() string {
	return ObjToString(&v)
}

type User struct {
	ID int `bson:"id"`
	//id of the private chat with the user
	PMID              int      `bson:"pmid"`
	Username          string   `bson:"username"`
	Name              string   `bson:"name"`

	NotificationDelay int      `bson:"-"`
	Labels            []string `bson:"-"`
	MentioningMethod  string   `bson:"-"`
	Lang              *string   `bson:"-"`
}

func (u User) String() string {
	return ObjToString(&u)
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
	return ObjToString(&n)
}

func ObjToString(obj interface{}) string {
	b, err := json.Marshal(obj)
	if err != nil {
		return fmt.Sprintf("cannot represent as json: %s", err)
	}
	return string(b)
}
