package clickhouse

import (
	"context"
	"net/http"
)

type ctxKey uint8

const (
	ctxTransportCallbackKey ctxKey = iota + 1
)

// TransportCallback is a transport response callback. Called before processing the http response.
type TransportCallback func(*http.Request, *http.Response) error

// CtxAddTransportCallback adds callback to work with transport response.
func CtxAddTransportCallback(ctx context.Context, f TransportCallback) context.Context {
	return context.WithValue(ctx, ctxTransportCallbackKey, f)
}

func callCtxTransportCallback(ctx context.Context, req *http.Request, resp *http.Response) error {
	if f, ok := ctx.Value(ctxTransportCallbackKey).(TransportCallback); ok && f != nil {
		return f(req, resp)
	}

	return nil
}
