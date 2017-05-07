package models

import (
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

type User struct {
	ID       int
	PMID     int
	Username string
	Name     string
}
