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

func CreateChat(ctx context.Context, chat *models.Chat) error {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("chat_id", chat.ID).Info("Creating the chat")
	conn, err := neo.GetDBConn()
	if err != nil {
		return err
	}
	params := map[string]interface{}{"chat_id": chat.ID, "title": chat.Title}
	err = conn.Exec(ctx, `MERGE (c: Chat {cid: {chat_id}) ON CREATE SET c.title={title}`, params)
	return err
}

func DeleteChat(ctx context.Context, chatID int) error {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("chat_id", chatID).Info("Deleting the chat")
	conn, err := neo.GetDBConn()
	if err != nil {
		return err
	}
	params := map[string]interface{}{"chat_id": chatID}
	err = conn.Exec(ctx, `MATCH (c: Chat) WHERE c.cid={chat_id} DELETE c`, params)
	return err
}

func RemoveUserFromChat(ctx context.Context, chatID, userID int) error {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithFields(log.Fields{"chat_id": chatID, "user_id": userID}).Info("Removing a user from chat")
	conn, err := neo.GetDBConn()
	if err != nil {
		return err
	}
	params := map[string]interface{}{"chat_id": chatID, "user_id": chatID}
	err = conn.Exec(ctx, `MATCH(u:User) -[m:Member]-> (c: Chat) WHERE u.uid={user_id} AND c.cid={chat_id} DELETE m`,
		params)
	return err
}

func AddUserToChat(ctx context.Context, chatID, userID int) error {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithFields(log.Fields{"chat_id": chatID, "user_id": userID}).Info("Adding a user to the chat")
	conn, err := neo.GetDBConn()
	if err != nil {
		return err
	}
	params := map[string]interface{}{"chat_id": chatID, "user_id": chatID}
	err = conn.Exec(ctx,
		`MATCH(u:User), (c: Chat) WHERE u.uid={user_id} AND c.cid={chat_id} CREATE (u)-[:Member]->(c)`, params)
	return err
}

func CreateUser(ctx context.Context, user *models.User) error {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithField("user_id", user.ID).Info("Get or create the user")
	conn, err := neo.GetDBConn()
	if err != nil {
		return err
	}
	params := map[string]interface{}{"user_id": user.ID, "name": user.Name, "pmid": user.PMID, "label": user.Name}
	err = conn.Exec(ctx, `MERGE (u: User {uid: {user_id}}) ON CREATE SET u.name={name},u.pmid={pmid},u.lbls=[{label}]`,
		params)
	return err
}

func AddLabelToUser(ctx context.Context, userID int, label string) error {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithFields(log.Fields{"user_id": userID, "label": label}).Info("Adding a new label to the user")
	conn, err := neo.GetDBConn()
	if err != nil {
		return err
	}
	params := map[string]interface{}{"user_id": userID, "label": label}
	err = conn.Exec(ctx, `MATCH (u: User {uid: {user_id}}) WHERE not {label} in u.lbls SET u.lbls=u.lbls + {label}`,
		params)
	return err
}

func RemoveLabelFromUser(ctx context.Context, userID int, label string) error {
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.WithFields(log.Fields{"user_id": userID, "label": label}).Info("Deleting user label")
	conn, err := neo.GetDBConn()
	if err != nil {
		return err
	}
	params := map[string]interface{}{"user_id": userID, "label": label}
	err = conn.Exec(ctx, `MATCH (u: User {uid: {user_id}}) SET u.lbls=FILTER (lbl IN u.lbls WHERE lbl<>{label})`,
		params)
	return err
}

func GetUserLabels(ctx context.Context, userID int) ([]string, error) {
	logger := logging.FromContextAndBase(ctx, gLogger).WithField("user_id", userID)
	logger.Info("Extracting user labels")
	conn, err := neo.GetDBConn()
	if err != nil {
		return nil, err
	}
	params := map[string]interface{}{"user_id": userID}
	row, err := conn.QueryOne(ctx, `MATCH (u: User {uid: {user_id}}) RETURN u.lbls`, params)
	if err != nil {
		if err == sql.ErrNoRows {
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

func FindUsersByLabel(ctx context.Context, chatID int, text string) ([]*models.User, error) {
	logger := logging.FromContextAndBase(ctx, gLogger).WithFields(log.Fields{"chat_id": chatID, "text": text})
	logger.Info("Getting users in the chat by label")
	conn, err := neo.GetDBConn()
	if err != nil {
		return nil, err
	}
	params := map[string]interface{}{"chat_id": chatID, "text": text}
	rows, err := conn.Query(ctx,
		`MATCH (u: User)-[:Member]->(c:Chat {cid:{chat_id}}) WHERE ANY(lbl IN u.lbls where {text} contains lbl) RETURN u`,
		params)
	var users []*models.User
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		user, err := rowToUser(ctx, row)
		if err != nil {
			return nil, errors.Wrap(err, "cannot convert row to User")
		}
		users = append(users, user)
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

func rowToUser(ctx context.Context, row []interface{}) (*models.User, error) {
	logger := logging.FromContextAndBase(ctx, gLogger)
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
	return &models.User{ID: userID, PMID: PMID, Name: name}, nil
}
