package clickhouse

import (
	"context"
	"net/http"
)

type ctxKey uint8

const (
	ctxRespCallbackKey ctxKey = iota + 1
)

// RespCallback is a transport response callback.
type RespCallback func(*http.Request, *http.Response) error

// CtxAddRespCallback adds callback to work with transport response.
func CtxAddRespCallback(ctx context.Context, f RespCallback) context.Context {
	return context.WithValue(ctx, ctxRespCallbackKey, f)
}

func callCtxRespCallback(ctx context.Context, req *http.Request, resp *http.Response) error {
	if f, ok := ctx.Value(ctxRespCallbackKey).(RespCallback); ok && f != nil {
		return f(req, resp)
	}

	return nil
}
