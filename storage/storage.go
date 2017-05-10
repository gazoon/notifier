package storage

import (
	"context"
	"notifier/logging"
	"notifier/models"
	"notifier/neo"

	"database/sql"

	log "github.com/Sirupsen/logrus"
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
	CreateUser(ctx context.Context, user *models.User) error
	AddLabelToUser(ctx context.Context, userID int, label string) error
	RemoveLabelFromUser(ctx context.Context, userID int, label string) error
	GetUserLabels(ctx context.Context, userID int) ([]string, error)
	FindUsersByLabel(ctx context.Context, chatID int, text string) ([]*models.User, error)
}
type neoStorage struct {
	client neo.Client
}

func NewNeoStorage(neoClient neo.Client) Storage {
	return &neoStorage{neoClient}
}

func (ns *neoStorage) CreateChat(ctx context.Context, chat *models.Chat) error {
	conn, err := ns.client.GetConn()
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	params := map[string]interface{}{"chat_id": chat.ID, "title": chat.Title}
	err = conn.Exec(ctx, `MERGE (c: Chat {cid: {chat_id}) ON CREATE SET c.title={title}`, params)
	return err
}

func (ns *neoStorage) DeleteChat(ctx context.Context, chatID int) error {
	conn, err := ns.client.GetConn()
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	params := map[string]interface{}{"chat_id": chatID}
	err = conn.Exec(ctx, `MATCH (c: Chat) WHERE c.cid={chat_id} DELETE c`, params)
	return err
}

func (ns *neoStorage) RemoveUserFromChat(ctx context.Context, chatID, userID int) error {
	conn, err := ns.client.GetConn()
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	params := map[string]interface{}{"chat_id": chatID, "user_id": chatID}
	err = conn.Exec(ctx, `MATCH(u:User) -[m:Member]-> (c: Chat) WHERE u.uid={user_id} AND c.cid={chat_id} DELETE m`,
		params)
	return err
}

func (ns *neoStorage) AddUserToChat(ctx context.Context, chatID, userID int) error {
	conn, err := ns.client.GetConn()
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	params := map[string]interface{}{"chat_id": chatID, "user_id": chatID}
	err = conn.Exec(ctx,
		`MATCH(u:User), (c: Chat) WHERE u.uid={user_id} AND c.cid={chat_id} MERGE (u)-[:Member]->(c)`, params)
	return err
}

func (ns *neoStorage) CreateUser(ctx context.Context, user *models.User) error {
	conn, err := ns.client.GetConn()
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	params := map[string]interface{}{"user_id": user.ID, "name": user.Name, "pmid": user.PMID, "label": user.Name}
	err = conn.Exec(ctx, `MERGE (u: User {uid: {user_id}}) ON CREATE SET u.name={name},u.pmid={pmid},u.lbls=[{label}]`,
		params)
	return err
}

func (ns *neoStorage) AddLabelToUser(ctx context.Context, userID int, label string) error {
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

func (ns *neoStorage) RemoveLabelFromUser(ctx context.Context, userID int, label string) error {
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

func (ns *neoStorage) GetUserLabels(ctx context.Context, userID int) ([]string, error) {
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
	labels, err := rowToLabels(ctx, row)
	if err != nil {
		return nil, errors.Wrap(err, "cannot convert row to labels")
	}
	return labels, nil
}

func (ns *neoStorage) FindUsersByLabel(ctx context.Context, chatID int, text string) ([]*models.User, error) {
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
	users, err := rowsToUsers(ctx, rows)
	if err != nil {
		return nil, errors.Wrap(err, "cannot convert rows to list of User models")
	}
	return users, nil
}

func rowToLabels(ctx context.Context, row []interface{}) ([]string, error) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	if len(row) != 1 {
		logger.WithField("row", row).Error("Expected row length is 1")
		return nil, errors.New("incorrect row length")
	}
	labels, ok := row[0].([]string)
	if !ok {
		logger.WithField("row_value", row[0]).Error("Expected row elem type is []string")
		return nil, errors.New("incorrect row elem type")
	}
	return labels, nil
}

func rowsToUsers(ctx context.Context, rows [][]interface{}) ([]*models.User, error) {
	logger := logging.FromContextAndBase(ctx, gLogger)
	var users []*models.User
	for _, row := range rows {
		if len(row) != 1 {
			logger.WithField("row", row).Error("Expected row length is 1")
			return nil, errors.New("incorrect row length")
		}
		data, ok := row[0].(map[string]interface{})
		if !ok {
			logger.WithField("row_value", row[0]).Error("Expected json-like row elemt")
			return nil, errors.New("incorrect row elem type")
		}
		userID, isUserIdOk := data["uid"].(int)
		PMID, isPmIdOk := data["pmid"].(int)
		name, isNameOk := data["name"].(string)
		if !isUserIdOk || !isPmIdOk || !isNameOk {
			logger.WithField("user_data", data).Errorf("Expected: uid, pmid - int, name - string")
			return nil, errors.New("incorrect user property")
		}
		users = append(users, &models.User{ID: userID, PMID: PMID, Name: name})
	}
	return users, nil
}
