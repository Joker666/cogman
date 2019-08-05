package util

import (
	"context"

	"github.com/streadway/amqp"
)

type Worker struct {
	taskName string
	handler  Handler
}

type Handler interface {
	Do(ctx context.Context, payload []byte) error
}

func NewWorker(name string, h Handler) *Worker {
	return &Worker{
		taskName: name,
		handler:  h,
	}
}

func (w *Worker) Name() string {
	return w.taskName
}

func (w *Worker) Process(msg *amqp.Delivery) error {
	h := Header{}
	for k, v := range msg.Headers {
		if s, ok := v.(string); ok {
			h.Set(k, s)
		}
	}
	return w.handler.Do(NewHeaderContext(context.Background(), h), msg.Body)
}

type HandlerFunc func(ctx context.Context, payload []byte) error

func (h HandlerFunc) Do(ctx context.Context, payload []byte) error {
	return h(ctx, payload)
}
