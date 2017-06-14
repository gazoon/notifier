package storage

import (
	"context"
	"notifier/libs/logging"
	"notifier/libs/models"
	"notifier/libs/neo"

	"reflect"

	"github.com/johnnadratowski/golang-neo4j-bolt-driver/structures/graph"
	"github.com/pkg/errors"
)

var (
	gLogger = logging.WithPackage("storage")
)

type Storage interface {
	CreateChat(ctx context.Context, chat *models.Chat) error
	DeleteChat(ctx context.Context, chatID int) error
	RemoveUserFromChat(ctx context.Context, chatID, userID int) error
	AddUserToChat(ctx context.Context, chatID, userID int) error
	GetOrCreateUser(ctx context.Context, user *models.User, notificationDelay int, mentioningMethod string,
		labels []string) error
	AddLabelToUser(ctx context.Context, userID int, label string) error
	SetNotificationDelay(ctx context.Context, userID, delay int) error
	SetMentioningMethod(ctx context.Context, userID int, method string) error
	RemoveLabelFromUser(ctx context.Context, userID int, label string) error
	GetChatUsers(ctx context.Context, chatID int) ([]*models.User, error)
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
	return ns.client.ExecRetry(ctx, `MATCH (u: User {uid: {user_id}}) SET u.notification_delay={delay}`, params)
}

func (ns *NeoStorage) SetMentioningMethod(ctx context.Context, userID int, method string) error {
	params := map[string]interface{}{"user_id": userID, "mentioning": method}
	return ns.client.ExecRetry(ctx, `MATCH (u: User {uid: {user_id}}) SET u.mentioning={mentioning}`, params)
}

func (ns *NeoStorage) CreateChat(ctx context.Context, chat *models.Chat) error {
	params := map[string]interface{}{"chat_id": chat.ID, "title": chat.Title}
	err := ns.client.ExecRetry(ctx, `MERGE (c: Chat {cid: {chat_id}}) ON CREATE SET c.title={title}`, params)
	return err
}

func (ns *NeoStorage) DeleteChat(ctx context.Context, chatID int) error {
	params := map[string]interface{}{"chat_id": chatID}
	err := ns.client.ExecRetry(ctx, `MATCH (c: Chat) WHERE c.cid={chat_id} DETACH DELETE c`, params)
	return err
}

func (ns *NeoStorage) RemoveUserFromChat(ctx context.Context, chatID, userID int) error {
	params := map[string]interface{}{"chat_id": chatID, "user_id": userID}
	err := ns.client.ExecRetry(ctx,
		`MATCH(u:User) -[m:Member]-> (c: Chat) WHERE u.uid={user_id} AND c.cid={chat_id} DELETE m`, params)
	return err
}

func (ns *NeoStorage) AddUserToChat(ctx context.Context, chatID, userID int) error {
	params := map[string]interface{}{"chat_id": chatID, "user_id": userID}
	err := ns.client.ExecRetry(ctx,
		`MATCH(u:User), (c: Chat) WHERE u.uid={user_id} AND c.cid={chat_id} MERGE (u)-[:Member]->(c)`, params)
	return err
}

func (ns *NeoStorage) GetOrCreateUser(ctx context.Context, user *models.User, notificationDelay int,
	mentioningMethod string, labels []string) error {

	labelsArg := make([]interface{}, len(labels))
	for i := range labels {
		labelsArg[i] = labels[i]
	}
	params := map[string]interface{}{"user_id": user.ID, "name": user.Name, "pmid": user.PMID, "labels": labelsArg,
		"delay": notificationDelay, "mentioning": mentioningMethod}
	row, err := ns.client.QueryOneRetry(ctx,
		`MERGE (u: User {uid: {user_id}}) ON CREATE SET
		u.name={name},u.pmid={pmid},u.lbls={labels},u.notification_delay={delay},u.mentioning={mentioning} return u`,
		params)
	if err != nil {
		return err
	}
	err = rowToUser(row, user)
	return errors.Wrap(err, "user deserialization failed")
}

func (ns *NeoStorage) AddLabelToUser(ctx context.Context, userID int, label string) error {
	params := map[string]interface{}{"user_id": userID, "label": label}
	err := ns.client.ExecRetry(ctx,
		`MATCH (u: User {uid: {user_id}}) WHERE not {label} in u.lbls SET u.lbls=u.lbls + {label}`, params)
	return err
}

func (ns *NeoStorage) RemoveLabelFromUser(ctx context.Context, userID int, label string) error {
	params := map[string]interface{}{"user_id": userID, "label": label}
	err := ns.client.ExecRetry(ctx,
		`MATCH (u: User {uid: {user_id}}) SET u.lbls=FILTER (lbl IN u.lbls WHERE lbl<>{label})`, params)
	return err
}

func (ns *NeoStorage) GetChatUsers(ctx context.Context, chatID int) ([]*models.User, error) {
	params := map[string]interface{}{"chat_id": chatID}
	rows, err := ns.client.Query(ctx,
		`MATCH (u: User)-[:Member]->(c:Chat {cid:{chat_id}}) RETURN u`,
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

	return nil
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
	if len(row) != 1 {
		return errors.Errorf("expected row length is 1, row = %v", row)
	}
	node, ok := row[0].(graph.Node)
	if !ok {
		return errors.Errorf("expected graph Node row element, got %v", row[0])
	}
	data := node.Properties
	userID, isUserIdOk := data["uid"].(int64)
	PMID, isPmIdOk := data["pmid"].(int64)
	name, isNameOk := data["name"].(string)
	notificationDelay, isDelayOk := data["notification_delay"].(int64)
	lbls, isLabelsOk := data["lbls"].([]interface{})
	mentioningMethod, isMentioningOk := data["mentioning"].(string)
	if !isUserIdOk || !isPmIdOk || !isNameOk || !isDelayOk || !isLabelsOk || !isMentioningOk {
		return errors.Errorf(
			"expected uid-int64,pmid-int64,name-string,delay-int64,labels-[]interface{},mentioning-string"+
				" properties, got %v %v %v %v %v %v",
			reflect.TypeOf(data["uid"]), reflect.TypeOf(data["pmid"]),
			reflect.TypeOf(data["name"]), reflect.TypeOf(data["notification_delay"]), reflect.TypeOf(data["lbls"]),
			reflect.TypeOf(data["mentioning"]))
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
	return nil
}
