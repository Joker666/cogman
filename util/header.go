package util

import (
	"context"
	"net/http"
)

// Header is http header
type Header http.Header

// Add adds a header value
func (h Header) Add(key, val string) {
	http.Header(h).Add(key, val)
}

// Set sets header value
func (h Header) Set(key, val string) {
	http.Header(h).Set(key, val)
}

// Get gets header value
func (h Header) Get(key string) string {
	return http.Header(h).Get(key)
}

// All returns all headers
func (h Header) All(key string) []string {
	return http.Header(h)[key]
}

type ctxKey int

const (
	ctxKeyHeader ctxKey = iota
)

// NewHeaderContext returns header context
func NewHeaderContext(ctx context.Context, header Header) context.Context {
	return context.WithValue(ctx, ctxKeyHeader, header)
}

// HeaderFromContext returns header from context
func HeaderFromContext(ctx context.Context) Header {
	v := ctx.Value(ctxKeyHeader)
	if v == nil {
		return Header{}
	}
	return v.(Header)
}
