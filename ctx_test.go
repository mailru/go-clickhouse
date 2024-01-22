package clickhouse

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_CtxAddRespCallback(t *testing.T) {
	var flag bool
	ctx := context.Background()

	ctx = CtxAddRespCallback(ctx, func(_ *http.Request, _ *http.Response) error {
		flag = true
		return nil
	})

	assert.NoError(t, callCtxRespCallback(ctx,
		httptest.NewRequest(http.MethodGet, "http://localhost", nil), httptest.NewRecorder().Result(),
	))
	assert.True(t, flag)
}

func Test_CtxAddRespCallback_err(t *testing.T) {
	var flag bool
	ctx := context.Background()

	ctx = CtxAddRespCallback(ctx, func(_ *http.Request, _ *http.Response) error {
		flag = true
		return errors.New("some error")
	})

	assert.EqualError(t, callCtxRespCallback(ctx,
		httptest.NewRequest(http.MethodGet, "http://localhost", nil), httptest.NewRecorder().Result(),
	), "some error")
	assert.True(t, flag)
}
