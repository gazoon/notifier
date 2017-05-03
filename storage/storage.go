package storage

import (
	"context"
	"notifier/logging"
	"notifier/models"
	"notifier/neo"

	log "github.com/Sirupsen/logrus"
	neoDriver "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"github.com/pkg/errors"
)

var (
	gLogger = logging.WithPackage("storage")
)

func getLogger(ctx context.Context) *log.Entry {
	return logging.FromContextAndBase(ctx, gLogger)
}

func getDBConn() (neoDriver.Conn, error) {
	db := neo.GetDB()
	conn, err := db.OpenPool()
	if err != nil {
		return nil, errors.WithMessage(err, "cannot get connection from pool")
	}
	return conn, err
}

func GetOrCreateChat(ctx context.Context, chat *models.Chat) error {
	logger := getLogger(ctx)
	logger.WithField("chat_id", chat.ID).Info("Get or create chat")
	conn, err := getDBConn()
	if err != nil {
		return
	}
	params := map[string]interface{}{"chat_id": chat.ID, "title": chat.Title}
	_, err = conn.ExecNeo(`MERGE (c: Chat {cid: {chat_id}) ON CREATE SET c.title={title}`, params)
	return err
}

func DeleteChat(ctx context.Context, chatID int) error {
	logger := getLogger(ctx)
	logger.WithField("chat_id", chatID).Info("Deleting the chat")
	conn, err := getDBConn()
	if err != nil {
		return err
	}
	params := map[string]interface{}{"chat_id": chatID}
	_, err = conn.ExecNeo(`MATCH (c: Chat) WHERE c.cid={chat_id} DELETE c`, params)
	return err
}

func RemoveUserFromChat(ctx context.Context, chatID, userID int) error {
	logger := getLogger(ctx)
	logger.WithFields(log.Fields{"chat_id": chatID, "user_id": userID}).Info("Removing a user from chat")
	conn, err := getDBConn()
	if err != nil {
		return err
	}
	params := map[string]interface{}{"chat_id": chatID, "user_id": chatID}
	_, err = conn.ExecNeo(`MATCH(u:User) -[m:Member]-> (c: Chat) WHERE u.uid={user_id} AND c.cid={chat_id} DELETE m`,
		params)
	return err
}

func AddUserToChat(ctx context.Context, chatID, userID int) error {
	logger := getLogger(ctx)
	logger.WithFields(log.Fields{"chat_id": chatID, "user_id": userID}).Info("Adding a user to the chat")
	conn, err := getDBConn()
	if err != nil {
		return err
	}
	params := map[string]interface{}{"chat_id": chatID, "user_id": chatID}
	_, err = conn.ExecNeo(
		`MATCH(u:User), (c: Chat) WHERE u.uid={user_id} AND c.cid={chat_id} CREATE (u)-[:Member]->(c)`, params)
	return err
}

func GetOrCreateUser(ctx context.Context, user *models.User) error {
	logger := getLogger(ctx)
	logger.WithField("user_id", user.ID).Info("Get or create the user")
	conn, err := getDBConn()
	if err != nil {
		return err
	}
	params := map[string]interface{}{"user_id": user.ID, "name": user.Name, "pmid": user.PMID, "label": user.Name}
	_, err = conn.ExecNeo(`MERGE (u: User {uid: {user_id}}) ON CREATE SET u.name={name},u.pmid={pmid},lbls=[{label}]`,
		params)
	return err
}

func AddLabelToUser(ctx context, userID int, label string) error {
	logger := getLogger(ctx)
	logger.WithFields(log.Fields{"user_id": userID, "label": label}).Info("Adding a new label to the user")
	conn, err := getDBConn()
	if err != nil {
		return err
	}
	params := map[string]interface{}{"user_id": userID, "label": label}
	_, err = conn.ExecNeo(`MATCH (u: User {uid: {user_id}}) WHERE not {label} in u.lbls SET u.lbls=u.lbls + {label}`,
		params)
	return err
}

func RemoveLabelFromUser(ctx context, userID int, label string) error {
	logger := getLogger(ctx)
	logger.WithFields(log.Fields{"user_id": userID, "label": label}).Info("Deleting user label")
	conn, err := getDBConn()
	if err != nil {
		return err
	}
	params := map[string]interface{}{"user_id": userID, "label": label}
	_, err = conn.ExecNeo(`MATCH (u: User {uid: {user_id}}) SET u.lbls=FILTER (lbl IN u.lbls WHERE lbl<>{label})`,
		params)
	return err
}

func GetUserLabels(ctx context, userID int) ([]string, error) {
	logger := getLogger(ctx)
	logger.WithFields(log.Fields{"user_id": userID}).Info("Extract user labels")
	conn, err := getDBConn()
	if err != nil {
		return nil, err
	}
	params := map[string]interface{}{"user_id": userID}
	_, err = conn.QueryNeo(`MATCH (u: User {uid: {user_id}}) RETURN u.lbls`, params)
	if err != nil {
		return nil, err
	}
	return nil, err
}
