package msgsqueue

import (
	"notifier/libs/logging"
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

type Voice struct {
	ID         string `bson:"id"`
	Duration   int    `bson:"duration"`
	Size       *int   `bson:"size"`
	Encoding   string `bson:"encoding"`
	SampleRate int    `bson:"sample_rate"`
}

func (v Voice) String() string {
	return logging.ObjToString(&v)
}

type User struct {
	ID int `bson:"id"`
	//id of the private chat with the user
	Username string `bson:"username"`
	Name     string `bson:"name"`
}
