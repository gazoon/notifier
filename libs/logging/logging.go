package logging

import (
	"strings"

	"context"
	"net/http"

	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/satori/go.uuid"
	"encoding/json"
)

var (
	gLogger = WithPackage("logging")
)

type ContextKey int

const (
	ServiceNameField = "service_name"
	ServerIDField    = "server_id"
	RequestIDField   = "request_id"
	PackageField     = "package"

	loggerCtxKey = ContextKey(1)
)

type customFormatter struct {
	logFormatter     log.Formatter
	additionalFields log.Fields
}

func NewRequestID() string {
	return uuid.NewV4().String()
}

func WithPackage(packageName string) *log.Entry {
	return log.WithField(PackageField, packageName)
}

func WithRequestID(requestID string) *log.Entry {
	return log.WithField(RequestIDField, requestID)
}

func WithRequestIDAndBase(requestID string, base *log.Entry) *log.Entry {
	requestLogger := WithRequestID(requestID)
	return base.WithFields(requestLogger.Data)
}

func (cf *customFormatter) Format(e *log.Entry) ([]byte, error) {
	data := make(log.Fields, len(e.Data)+len(cf.additionalFields))
	for k, v := range e.Data {
		data[k] = v
	}
	for k, v := range cf.additionalFields {
		data[k] = v
	}
	var newEntry = new(log.Entry)
	*newEntry = *e
	newEntry.Data = data
	return cf.logFormatter.Format(newEntry)
}

func FromContext(ctx context.Context) *log.Entry {
	logger, ok := ctx.Value(loggerCtxKey).(*log.Entry)
	if !ok {
		logger = log.NewEntry(log.StandardLogger())
	}
	return logger
}

func FromContextAndBase(ctx context.Context, base *log.Entry) *log.Entry {
	ctxLogger := FromContext(ctx)
	return base.WithFields(ctxLogger.Data)
}

func NewContext(ctx context.Context, logger *log.Entry) context.Context {
	return context.WithValue(ctx, loggerCtxKey, logger)
}

func NewContextBackground(logger *log.Entry) context.Context {
	return NewContext(context.Background(), logger)
}

func NewFormatter(serviceName, serverID string) log.Formatter {
	formatter := &customFormatter{logFormatter: &log.TextFormatter{}, additionalFields: log.Fields{
		ServiceNameField: serviceName,
		ServerIDField:    serverID,
	}}
	return formatter
}

func getLogLevel(logLevelName string) log.Level {
	var logLevel log.Level
	switch strings.ToLower(logLevelName) {
	case "debug":
		logLevel = log.DebugLevel
	case "info":
		logLevel = log.InfoLevel
	case "warning":
		logLevel = log.WarnLevel
	case "error":
		logLevel = log.ErrorLevel
	default:
		logLevel = log.InfoLevel
	}
	return logLevel
}

func PatchStdLog(logLevelName, serviceName, serverID string) {
	logLevel := getLogLevel(logLevelName)
	formatter := NewFormatter(serviceName, serverID)
	log.SetLevel(logLevel)
	log.SetFormatter(formatter)
}


func StartLevelToggle(togglePath string, port int) {
	mux := http.NewServeMux()
	mux.HandleFunc(togglePath, func(w http.ResponseWriter, r *http.Request) {
		levelName := r.FormValue("level")
		level := getLogLevel(levelName)
		gLogger.Infof("Toggle global log level from %s to %s", log.GetLevel(), level)
		log.SetLevel(level)
		fmt.Fprint(w, level)
	})
	gLogger.Infof("Toggle server is running on %d port, path=%s", port, togglePath)
	go func() {
		http.ListenAndServe(fmt.Sprintf(":%d", port), mux)
	}()
}

func ObjToString(obj interface{}) string {
	b, err := json.Marshal(obj)
	if err != nil {
		return fmt.Sprintf("cannot represent as json: %s", err)
	}
	return string(b)
}
