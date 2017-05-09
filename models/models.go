package models

import (
	"encoding/json"
	"fmt"
	"strings"
)

type Message struct {
	ID             int
	RequestID      string
	Chat           *Chat
	Text           string
	From           *User
	NewChatMember  *User
	LeftChatMember *User
	IsBotLeft      bool
	IsBotAdded     bool
}

func toString(model interface{}) string {
	b, err := json.Marshal(model)
	if err != nil {
		return fmt.Sprintf("cannot represent as json: %s", err)
	}
	return string(b)
}

func (m Message) String() string {
	return toString(&m)
}

func (m *Message) ToCommand() (string, string) {
	text := strings.TrimSpace(m.Text)
	parts := strings.SplitN(text, " ", 1)
	cmd := strings.TrimSpace(parts[0])
	args := ""
	if len(parts) > 1 {
		args = strings.TrimSpace(parts[0])
	}
	return cmd, args
}

type Chat struct {
	ID        int
	IsPrivate bool
	Title     string
}

func (c Chat) String() string {
	return toString(&c)
}

type User struct {
	ID       int
	PMID     int
	Username string
	Name     string
}

func (u User) String() string {
	return toString(&u)
}
