package storage

import (
	"context"
	"notifier/libs/logging"
	"notifier/libs/models"
	"notifier/libs/neo"

	"database/sql"

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
	CreateUser(ctx context.Context, user *models.User, labels []string) error
	AddLabelToUser(ctx context.Context, userID int, label string) error
	SetNotificationDelay(ctx context.Context, userID, delay int) error
	RemoveLabelFromUser(ctx context.Context, userID int, label string) error
	GetUserLabels(ctx context.Context, userID int) ([]string, error)
	FindUsersByLabel(ctx context.Context, chatID int, text string) ([]*models.User, error)
}
type NeoStorage struct {
	client *neo.Client
}

func NewNeoStorage(host string, port int, user, password string, timeout, poolSize int) (*NeoStorage, error) {
	neoDB, err := neo.NewClient(host, port, user, password, timeout, poolSize)
	if err != nil {
		return nil, err
	}
	return &NeoStorage{neoDB}, nil
}

func (ns *NeoStorage) SetNotificationDelay(ctx context.Context, userID, delay int) error {
	conn, err := ns.client.GetConn()
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	params := map[string]interface{}{"user_id": userID, "delay": delay}
	err = conn.Exec(ctx, `MATCH (u: User {uid: {user_id}}) SET u.notification_delay={delay}`, params)
	return err
}

func (ns *NeoStorage) CreateChat(ctx context.Context, chat *models.Chat) error {
	conn, err := ns.client.GetConn()
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	params := map[string]interface{}{"chat_id": chat.ID, "title": chat.Title}
	err = conn.Exec(ctx, `MERGE (c: Chat {cid: {chat_id}}) ON CREATE SET c.title={title}`, params)
	return err
}

func (ns *NeoStorage) DeleteChat(ctx context.Context, chatID int) error {
	conn, err := ns.client.GetConn()
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	params := map[string]interface{}{"chat_id": chatID}
	err = conn.Exec(ctx, `MATCH (c: Chat) WHERE c.cid={chat_id} DETACH DELETE c`, params)
	return err
}

func (ns *NeoStorage) RemoveUserFromChat(ctx context.Context, chatID, userID int) error {
	conn, err := ns.client.GetConn()
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	params := map[string]interface{}{"chat_id": chatID, "user_id": userID}
	err = conn.Exec(ctx, `MATCH(u:User) -[m:Member]-> (c: Chat) WHERE u.uid={user_id} AND c.cid={chat_id} DELETE m`,
		params)
	return err
}

func (ns *NeoStorage) AddUserToChat(ctx context.Context, chatID, userID int) error {
	conn, err := ns.client.GetConn()
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	params := map[string]interface{}{"chat_id": chatID, "user_id": userID}
	err = conn.Exec(ctx,
		`MATCH(u:User), (c: Chat) WHERE u.uid={user_id} AND c.cid={chat_id} MERGE (u)-[:Member]->(c)`, params)
	return err
}

func (ns *NeoStorage) CreateUser(ctx context.Context, user *models.User, labels []string) error {
	conn, err := ns.client.GetConn()
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	labelsArg := make([]interface{}, len(labels))
	for i := range labels {
		labelsArg[i] = labels[i]
	}
	params := map[string]interface{}{"user_id": user.ID, "name": user.Name, "pmid": user.PMID, "labels": labelsArg,
		"delay": user.NotificationDelay}
	err = conn.Exec(ctx,
		`MERGE (u: User {uid: {user_id}}) ON CREATE SET u.name={name},u.pmid={pmid},u.lbls={labels},u.notification_delay={delay}`,
		params)
	return err
}

func (ns *NeoStorage) AddLabelToUser(ctx context.Context, userID int, label string) error {
	conn, err := ns.client.GetConn()
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	params := map[string]interface{}{"user_id": userID, "label": label}
	err = conn.Exec(ctx, `MATCH (u: User {uid: {user_id}}) WHERE not {label} in u.lbls SET u.lbls=u.lbls + {label}`,
		params)
	return err
}

func (ns *NeoStorage) RemoveLabelFromUser(ctx context.Context, userID int, label string) error {
	conn, err := ns.client.GetConn()
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	params := map[string]interface{}{"user_id": userID, "label": label}
	err = conn.Exec(ctx, `MATCH (u: User {uid: {user_id}}) SET u.lbls=FILTER (lbl IN u.lbls WHERE lbl<>{label})`,
		params)
	return err
}

func (ns *NeoStorage) GetUserLabels(ctx context.Context, userID int) ([]string, error) {
	conn, err := ns.client.GetConn()
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)
	params := map[string]interface{}{"user_id": userID}
	row, err := conn.QueryOne(ctx, `MATCH (u: User {uid: {user_id}}) RETURN u.lbls`, params)
	if err != nil {
		if err == sql.ErrNoRows {
			logger := logging.FromContextAndBase(ctx, gLogger).WithField("user_id", userID)
			logger.Warn("User not found, return empty list of labels")
			return nil, nil
		}
		return nil, err
	}
	labels, err := rowToLabels(row)
	if err != nil {
		return nil, errors.Wrap(err, "cannot convert row to labels")
	}
	return labels, nil
}

func (ns *NeoStorage) FindUsersByLabel(ctx context.Context, chatID int, text string) ([]*models.User, error) {
	conn, err := ns.client.GetConn()
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)
	params := map[string]interface{}{"chat_id": chatID, "text": text}
	rows, err := conn.Query(ctx,
		`MATCH (u: User)-[:Member]->(c:Chat {cid:{chat_id}}) WHERE ANY(lbl IN u.lbls where {text} contains lbl) RETURN u`,
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
	conn, err := ns.client.GetConn()
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	err = conn.Exec(ctx, `CREATE CONSTRAINT ON (u:User) ASSERT u.uid IS UNIQUE`, nil)
	if err != nil {
		return errors.Wrap(err, "User uid index")
	}

	err = conn.Exec(ctx, `CREATE CONSTRAINT ON (c:Chat) ASSERT c.cid IS UNIQUE`, nil)
	if err != nil {
		return errors.Wrap(err, "Chat cid index")
	}

	return nil
}

func rowToLabels(row []interface{}) ([]string, error) {
	if len(row) != 1 {
		return nil, errors.Errorf("expected row length is 1, row = %v", row)
	}
	elements, ok := row[0].([]interface{})
	if !ok {
		return nil, errors.Errorf("row elem must have []interface{} type, got %v", reflect.TypeOf(row[0]))
	}
	var labels []string
	for _, elem := range elements {
		label, ok := elem.(string)
		if !ok {
			return nil, errors.Errorf("label must be a string, got %v", reflect.TypeOf(elem))
		}
		labels = append(labels, label)
	}
	return labels, nil
}

func rowsToUsers(rows [][]interface{}) ([]*models.User, error) {
	var users []*models.User
	for _, row := range rows {
		if len(row) != 1 {
			return nil, errors.Errorf("expected row length is 1, row = %v", row)
		}
		node, ok := row[0].(graph.Node)
		if !ok {
			return nil, errors.Errorf("expected graph Node row element, got %v", row[0])
		}
		data := node.Properties
		userID, isUserIdOk := data["uid"].(int64)
		PMID, isPmIdOk := data["pmid"].(int64)
		name, isNameOk := data["name"].(string)
		notificationDelay, isDelayOk := data["notification_delay"].(int64)
		if !isUserIdOk || !isPmIdOk || !isNameOk || !isDelayOk {
			return nil, errors.Errorf("expected uid-int64,pmid-int64,name-string,delay-int64 properties, got %v %v %v %v",
				reflect.TypeOf(data["uid"]), reflect.TypeOf(data["pmid"]),
				reflect.TypeOf(data["name"]), reflect.TypeOf(data["notification_delay"]))
		}
		users = append(users, &models.User{
			ID:                int(userID),
			PMID:              int(PMID),
			Name:              name,
			NotificationDelay: int(notificationDelay),
		})
	}
	return users, nil
}
