package util

import (
	"context"

	"github.com/streadway/amqp"
)

// Handler is the task handler struct interface
type Handler interface {
	Do(ctx context.Context, payload []byte) error
}

// HandlerFunc is a task handler function type
type HandlerFunc func(ctx context.Context, payload []byte) error

// Do actually does the work
func (h HandlerFunc) Do(ctx context.Context, payload []byte) error {
	return h(ctx, payload)
}

// Worker hold the necessary field of task handler
type Worker struct {
	taskName string
	handler  Handler
}

// NewWorker return a new worker instance
func NewWorker(name string, h Handler) *Worker {
	return &Worker{
		taskName: name,
		handler:  h,
	}
}

// Name return worker name
func (w *Worker) Name() string {
	return w.taskName
}

// Handler return task handler
func (w *Worker) Handler() Handler {
	return w.handler
}

// Process call do method of task handler
func (w *Worker) Process(msg *amqp.Delivery) error {
	h := Header{}
	for k, v := range msg.Headers {
		if s, ok := v.(string); ok {
			h.Set(k, s)
		}
	}
	return w.handler.Do(NewHeaderContext(context.Background(), h), msg.Body)
}
