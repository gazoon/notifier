package storage

import (
	"context"
	"notifier/libs/logging"
	"notifier/libs/models"
	"notifier/libs/neo"

	"reflect"

	"database/sql"

	"github.com/johnnadratowski/golang-neo4j-bolt-driver/structures/graph"
	"github.com/pkg/errors"
)

var (
	gLogger = logging.WithPackage("storage")
)

type Storage interface {
	GetOrCreateChat(ctx context.Context, chat *models.Chat) error
	SetChatLang(ctx context.Context, chatID int, lang string) error
	DeleteChat(ctx context.Context, chatID int) error
	RemoveUserFromChat(ctx context.Context, chatID, userID int) error
	AddUserToChat(ctx context.Context, chatID, userID int) error
	GetChatUsers(ctx context.Context, chatID int) ([]*models.User, error)

	EnableChatWord(ctx context.Context, chatID int, word string) error
	DisableChatWord(ctx context.Context, chatID int, word string) error
	FilterSwearWordsForChat(ctx context.Context, chatID int, words []string) ([]string, error)
	GetChatEnabledWords(ctx context.Context, chatID int) ([]string, error)
	GetChatDisabledWords(ctx context.Context, chatID int) ([]string, error)

	GetOrCreateUser(ctx context.Context, user *models.User, pmid int) error
	GetUser(ctx context.Context, user *models.User) (bool, error)
	AddLabelToUser(ctx context.Context, userID int, label string) error
	RemoveLabelFromUser(ctx context.Context, userID int, label string) error
	SetNotificationDelay(ctx context.Context, userID, delay int) error
	SetMentioningMethod(ctx context.Context, userID int, method string) error
	SetCanDeleteNotifications(ctx context.Context, userID int, canDelete bool) error
}

type NeoStorage struct {
	client *neo.Client
}

func NewNeoStorage(host string, port int, user, password string, timeout, poolSize, retriesNum, retriesInterval int) (
	*NeoStorage, error) {

	neoDB, err := neo.NewClient(host, port, user, password, timeout, poolSize, retriesNum, retriesInterval)
	if err != nil {
		return nil, err
	}
	return &NeoStorage{neoDB}, nil
}

func (ns *NeoStorage) SetNotificationDelay(ctx context.Context, userID, delay int) error {
	params := map[string]interface{}{"user_id": userID, "delay": delay}
	return ns.client.ExecRetry(ctx, `MATCH (u:User {uid: {user_id}}) SET u.notification_delay={delay}`, params)
}

func (ns *NeoStorage) SetMentioningMethod(ctx context.Context, userID int, method string) error {
	params := map[string]interface{}{"user_id": userID, "mentioning": method}
	return ns.client.ExecRetry(ctx, `MATCH (u:User {uid: {user_id}}) SET u.mentioning={mentioning}`, params)
}

func (ns *NeoStorage) SetCanDeleteNotifications(ctx context.Context, userID int, canDelete bool) error {
	params := map[string]interface{}{"user_id": userID, "can_delete": canDelete}
	return ns.client.ExecRetry(ctx, `MATCH (u:User {uid: {user_id}}) SET u.delete_notifications={can_delete}`, params)
}

func (ns *NeoStorage) GetOrCreateChat(ctx context.Context, chat *models.Chat) error {
	params := map[string]interface{}{"chat_id": chat.ID, "title": chat.Title, "lang": models.DefaultLang}
	row, err := ns.client.QueryOneRetry(ctx,
		`MERGE (c:Chat {cid: {chat_id}}) ON CREATE SET c.title={title},c.lang={lang} RETURN c`, params)
	if err != nil {
		return err
	}
	err = rowToChat(row, chat)
	return errors.Wrap(err, "chat deserialization failed")
}

func (ns *NeoStorage) SetChatLang(ctx context.Context, chatID int, lang string) error {
	params := map[string]interface{}{"chat_id": chatID, "lang": lang}
	return ns.client.ExecRetry(ctx, `MATCH (c:Chat {cid: {chat_id}}) SET c.lang={lang}`, params)
}

func (ns *NeoStorage) DeleteChat(ctx context.Context, chatID int) error {
	params := map[string]interface{}{"chat_id": chatID}
	err := ns.client.ExecRetry(ctx, `MATCH (c:Chat) WHERE c.cid={chat_id} DETACH DELETE c`, params)
	return err
}

func (ns *NeoStorage) RemoveUserFromChat(ctx context.Context, chatID, userID int) error {
	params := map[string]interface{}{"chat_id": chatID, "user_id": userID}
	err := ns.client.ExecRetry(ctx,
		`MATCH(u:User) -[m:Member]-> (c:Chat) WHERE u.uid={user_id} AND c.cid={chat_id} DELETE m`, params)
	return err
}

func (ns *NeoStorage) AddUserToChat(ctx context.Context, chatID, userID int) error {
	params := map[string]interface{}{"chat_id": chatID, "user_id": userID}
	err := ns.client.ExecRetry(ctx,
		`MATCH(u:User), (c:Chat) WHERE u.uid={user_id} AND c.cid={chat_id} MERGE (u)-[:Member]->(c)`, params)
	return err
}

func (ns *NeoStorage) GetOrCreateUser(ctx context.Context, user *models.User, pmid int) error {
	labels := user.DefaultLabels()
	labelsArg := make([]interface{}, len(labels))
	for i := range labels {
		labelsArg[i] = labels[i]
	}
	params := map[string]interface{}{
		"user_id":    user.ID,
		"name":       user.Name,
		"pmid":       pmid,
		"labels":     labelsArg,
		"delay":      models.DefaultNotificationDelay,
		"mentioning": models.DefaultMentioningMethod,
		"can_delete": models.DefaultDeleteNotificationsFlag,
	}
	row, err := ns.client.QueryOneRetry(ctx,
		`MERGE (u:User {uid: {user_id}}) ON CREATE SET
		u.name={name},u.pmid={pmid},u.lbls={labels},u.notification_delay={delay},u.mentioning={mentioning},
		u.delete_notifications={can_delete} RETURN u`,
		params)
	if err != nil {
		return err
	}
	err = rowToUser(row, user)
	return errors.Wrap(err, "user deserialization failed")
}

func (ns *NeoStorage) GetUser(ctx context.Context, user *models.User) (bool, error) {
	params := map[string]interface{}{"user_id": user.ID}
	row, err := ns.client.QueryOneRetry(ctx, `MATCH (u:User {uid: {user_id}}) RETURN u`, params)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	err = rowToUser(row, user)
	if err != nil {
		return false, errors.Wrap(err, "user deserialization failed")
	}
	return true, nil
}

func (ns *NeoStorage) AddLabelToUser(ctx context.Context, userID int, label string) error {
	params := map[string]interface{}{"user_id": userID, "label": label}
	err := ns.client.ExecRetry(ctx,
		`MATCH (u:User {uid: {user_id}}) WHERE not {label} in u.lbls SET u.lbls=u.lbls + {label}`, params)
	return err
}

func (ns *NeoStorage) RemoveLabelFromUser(ctx context.Context, userID int, label string) error {
	params := map[string]interface{}{"user_id": userID, "label": label}
	err := ns.client.ExecRetry(ctx,
		`MATCH (u:User {uid: {user_id}}) SET u.lbls=FILTER (lbl IN u.lbls WHERE lbl<>{label})`, params)
	return err
}

func (ns *NeoStorage) GetChatUsers(ctx context.Context, chatID int) ([]*models.User, error) {
	params := map[string]interface{}{"chat_id": chatID}
	rows, err := ns.client.Query(ctx,
		`MATCH (u:User)-[:Member]->(c:Chat {cid:{chat_id}}) RETURN u`,
		params)
	if err != nil {
		return nil, err
	}
	users, err := rowsToUsers(rows)
	if err != nil {
		return nil, errors.Wrap(err, "cannot convert rows to list of User models")
	}
	return users, nil
}

func (ns *NeoStorage) CreateSwearWord(ctx context.Context, word string) error {
	params := map[string]interface{}{"word": word}
	err := ns.client.ExecRetry(ctx, `MERGE (w:SwearWord {word: {word}})`, params)
	return err
}

func (ns *NeoStorage) EnableChatWord(ctx context.Context, chatID int, word string) error {
	params := map[string]interface{}{"chat_id": chatID, "word": word}
	err := ns.client.ExecRetry(ctx,
		`MERGE (w:SwearWord {word: {word}}) WITH w MATCH (c:Chat {cid: {chat_id}})
		MERGE (c)-[:WordEnabled]->(w) WITH c,w
		MATCH (c)-[wd:WordDisabled]->(w) DELETE wd`, params)
	return err
}

func (ns *NeoStorage) DisableChatWord(ctx context.Context, chatID int, word string) error {
	params := map[string]interface{}{"chat_id": chatID, "word": word}
	err := ns.client.ExecRetry(ctx,
		`MATCH (w:SwearWord {word: {word}}) MATCH (c:Chat {cid: {chat_id}})
		MERGE (c)-[:WordDisabled]->(w) WITH c,w
		MATCH (c)-[we:WordEnabled]->(w) DELETE we`, params)
	return err
}

func (ns *NeoStorage) FilterSwearWordsForChat(ctx context.Context, chatID int, words []string) ([]string, error) {
	wordsArg := make([]interface{}, len(words))
	for i := range words {
		wordsArg[i] = words[i]
	}
	params := map[string]interface{}{"chat_id": chatID, "words": wordsArg}
	rows, err := ns.client.QueryRetry(ctx,
		`MATCH (c:Chat {cid: {chat_id}}) MATCH (w:SwearWord)
		WHERE w.word IN {words} AND NOT (c)-[:WordDisabled]->(w) RETURN w.word`, params)
	if err != nil {
		return nil, err
	}
	return rowsToStrings(rows)
}

func (ns *NeoStorage) GetChatEnabledWords(ctx context.Context, chatID int) ([]string, error) {
	params := map[string]interface{}{"chat_id": chatID}
	rows, err := ns.client.QueryRetry(ctx,
		`MATCH (c:Chat {cid: {chat_id}})-[:WordEnabled]->(w:SwearWord) RETURN w.word`, params)
	if err != nil {
		return nil, err
	}
	return rowsToStrings(rows)
}

func (ns *NeoStorage) GetChatDisabledWords(ctx context.Context, chatID int) ([]string, error) {
	params := map[string]interface{}{"chat_id": chatID}
	rows, err := ns.client.QueryRetry(ctx,
		`MATCH (c:Chat {cid: {chat_id}})-[:WordDisabled]->(w:SwearWord) RETURN w.word`, params)
	if err != nil {
		return nil, err
	}
	return rowsToStrings(rows)
}

func (ns *NeoStorage) PrepareIndexes() error {
	ctx := context.Background()
	var err error

	err = ns.client.Exec(ctx, `CREATE CONSTRAINT ON (u:User) ASSERT u.uid IS UNIQUE`, nil)
	if err != nil {
		return errors.Wrap(err, "User uid index")
	}

	err = ns.client.Exec(ctx, `CREATE CONSTRAINT ON (c:Chat) ASSERT c.cid IS UNIQUE`, nil)
	if err != nil {
		return errors.Wrap(err, "Chat cid index")
	}

	err = ns.client.Exec(ctx, `CREATE CONSTRAINT ON (w:SwearWord) ASSERT w.word IS UNIQUE`, nil)
	if err != nil {
		return errors.Wrap(err, "SwearWord word index")
	}

	return nil
}

func rowsToStrings(rows [][]interface{}) ([]string, error) {
	values := make([]string, len(rows))
	for i, row := range rows {
		if len(row) != 1 {
			return nil, errors.Errorf("expected row length is 1, row = %v", row)
		}
		value, ok := row[0].(string)
		if !ok {
			return nil, errors.Errorf("expected string row element, got %v", row[0])
		}
		values[i] = value
	}
	return values, nil
}

func rowToGraphNode(row []interface{}) (*graph.Node, error) {
	if len(row) != 1 {
		return nil, errors.Errorf("expected row length is 1, row = %v", row)
	}
	node, ok := row[0].(graph.Node)
	if !ok {
		return nil, errors.Errorf("expected graph Node row element, got %v", row[0])
	}
	return &node, nil
}

func rowsToUsers(rows [][]interface{}) ([]*models.User, error) {
	var users []*models.User
	for _, row := range rows {
		user := new(models.User)
		err := rowToUser(row, user)
		if err != nil {
			return nil, err
		}
		users = append(users, user)
	}
	return users, nil
}

func rowToUser(row []interface{}, user *models.User) error {
	node, err := rowToGraphNode(row)
	if err != nil {
		return err
	}

	data := node.Properties
	userID, isUserIdOk := data["uid"].(int64)
	PMID, isPmIdOk := data["pmid"].(int64)
	name, isNameOk := data["name"].(string)
	notificationDelay, isDelayOk := data["notification_delay"].(int64)
	lbls, isLabelsOk := data["lbls"].([]interface{})
	mentioningMethod, isMentioningOk := data["mentioning"].(string)
	deleteNotifications, isDeleteNotificationsOk := data["delete_notifications"].(bool)
	if !isUserIdOk || !isPmIdOk || !isNameOk || !isDelayOk || !isLabelsOk || !isMentioningOk || !isDeleteNotificationsOk {
		return errors.Errorf(
			"expected uid-int64,pmid-int64,name-string,delay-int64,labels-[]interface{},mentioning-string,delete_notifications-bool"+
				" properties, got %v %v %v %v %v %v %v",
			reflect.TypeOf(data["uid"]), reflect.TypeOf(data["pmid"]),
			reflect.TypeOf(data["name"]), reflect.TypeOf(data["notification_delay"]), reflect.TypeOf(data["lbls"]),
			reflect.TypeOf(data["mentioning"]), reflect.TypeOf(data["delete_notifications"]))
	}

	labels := make([]string, len(lbls))
	for i, item := range lbls {
		label, ok := item.(string)
		if !ok {
			return errors.Errorf("label must be a string, got %v", reflect.TypeOf(item))
		}
		labels[i] = label
	}

	user.ID = int(userID)
	user.PMID = int(PMID)
	user.Name = name
	user.NotificationDelay = int(notificationDelay)
	user.MentioningMethod = mentioningMethod
	user.Labels = labels
	user.CanDeleteNotifications = deleteNotifications
	return nil
}

func rowToChat(row []interface{}, chat *models.Chat) error {
	node, err := rowToGraphNode(row)
	if err != nil {
		return err
	}

	data := node.Properties
	chatID, isChatIdOk := data["cid"].(int64)
	title, isTitleOk := data["title"].(string)
	lang, isLangOk := data["lang"].(string)
	if !isChatIdOk || !isTitleOk || !isLangOk {
		return errors.Errorf(
			"expected cid-int64,title-string,lang-string chat properties, got %v %v %v",
			reflect.TypeOf(data["cid"]), reflect.TypeOf(data["title"]), reflect.TypeOf(data["lang"]))
	}

	chat.ID = int(chatID)
	chat.Title = title
	chat.Lang = lang
	return nil
}
