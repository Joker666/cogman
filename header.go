package cogman

import (
	"context"
	"net/http"
)

type Header http.Header

func (h Header) Add(key, val string) {
	http.Header(h).Add(key, val)
}

func (h Header) Set(key, val string) {
	http.Header(h).Set(key, val)
}

func (h Header) Get(key string) string {
	return http.Header(h).Get(key)
}

func (h Header) All(key string) []string {
	return http.Header(h)[key]
}

type ctxKey int

const (
	ctxKeyHeader ctxKey = iota
)

func NewHeaderContext(ctx context.Context, header Header) context.Context {
	return context.WithValue(ctx, ctxKeyHeader, header)
}

func HeaderFromContext(ctx context.Context) Header {
	v := ctx.Value(ctxKeyHeader)
	if v == nil {
		return Header{}
	}
	return v.(Header)
}
