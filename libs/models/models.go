package models

import (
	"notifier/libs/logging"
	"strings"
	"time"
)

const (
	AllMentioningMethod            = "all"
	NoneMentioningMethod           = "none"
	VoiceMentioningMethod          = "voice"
	TextMentioningMethod           = "text"
	DefaultMentioningMethod        = AllMentioningMethod
	DefaultNotificationDelay       = 10
	DefaultDeleteNotificationsFlag = true
)

var (
	MentioningMethodsList = [...]string{
		AllMentioningMethod, NoneMentioningMethod, VoiceMentioningMethod, TextMentioningMethod,
	}
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

	Lang string `bson:"-"`
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

	PMID                   int      `bson:"pmid"`
	NotificationDelay      int      `bson:"-"`
	CanDeleteNotifications bool     `bson:"-"`
	Labels                 []string `bson:"-"`
	MentioningMethod       string   `bson:"-"`
	Lang                   *string  `bson:"-"`
}

func (u User) String() string {
	return logging.ObjToString(&u)
}

func (u *User) DefaultLabels() []string {
	labelFromName := ProcessWord(u.Name)
	return []string{labelFromName}
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

func ExcludeUserFromList(users []*User, userToExclude *User) []*User {
	var result []*User
	for _, u := range users {
		if u.ID == userToExclude.ID {
			continue
		}
		result = append(result, u)
	}
	return result
}

func ProcessWord(word string) string {
	return strings.ToLower(word)
}

func ProcessWords(words []string) []string {
	result := make([]string, len(words))
	for i, text := range words {
		result[i] = ProcessWord(text)
	}
	return result
}

func GetMentionedUsers(users []*User, words []string) []*User {
	var mentioned []*User
	for _, user := range users {
		for _, label := range user.Labels {
			if isLabelInWords(words, label) {
				mentioned = append(mentioned, user)
				break
			}
		}
	}
	return mentioned
}

func isLabelInWords(words []string, label string) bool {
	for _, word := range words {
		if strings.HasPrefix(word, label) {
			return true
		}
	}
	return false
}

func EnumLabels(users []*User) []string {
	var labels []string
	for _, user := range users {
		labels = append(labels, user.Labels...)
	}
	return labels
}

func IsValidMentioningMethod(value string) bool {
	for _, method := range MentioningMethodsList {
		if method == value {
			return true
		}
	}
	return false
}

func FilterByMentioningMethod(users []*User, method string) []*User {
	var filtered []*User
	for _, user := range users {
		if user.MentioningMethod == method || user.MentioningMethod == AllMentioningMethod {
			filtered = append(filtered, user)
		}
	}
	return filtered
}
