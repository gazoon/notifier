package tracing

import (
	"context"

	"github.com/satori/go.uuid"
)

type ContextKey int

const (
	requestIDCtxKey = ContextKey(228)
)

func NewRequestID() string {
	return uuid.NewV4().String()
}

func NewContext(ctx context.Context, reqID string) context.Context {
	return context.WithValue(ctx, requestIDCtxKey, reqID)
}

func FromContext(ctx context.Context) string {
	reqID, _ := ctx.Value(requestIDCtxKey).(string)
	return reqID
}
