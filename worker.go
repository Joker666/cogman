package cogman

import (
	"context"

	"github.com/streadway/amqp"
)

type worker struct {
	taskName string
	handler  Handler
}

func (w *worker) process(msg *amqp.Delivery) error {
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
