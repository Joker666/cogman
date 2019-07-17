package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Tapfury/cogman/config"
	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

// Session holds necessery fields of a client session
type Session struct {
	cfg *config.Client

	mu        sync.RWMutex
	connected bool

	conn *amqp.Connection

	done   chan struct{}
	reconn chan *amqp.Error

	queueIndex int
}

// NewSession creates new client session with config cfg
func NewSession(cfg config.Client) (*Session, error) {
	if cfg.ConnectionTimeout < 0 || cfg.RequestTimeout < 0 {
		return nil, ErrInvalidConfig
	}
	return &Session{
		cfg:        &cfg,
		queueIndex: 0,
	}, nil
}

// Close closes session s
func (s *Session) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.connected = false

	close(s.done)

	return s.conn.Close()
}

// Connect connects a client session
func (s *Session) Connect() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.connected {
		return nil
	}

	s.done = make(chan struct{})

	ctx := context.Background()
	var cancel context.CancelFunc

	if s.cfg.ConnectionTimeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, s.cfg.ConnectionTimeout)
		defer cancel()
	}

	err := s.connect(ctx)
	if err != nil {
		return err
	}

	s.connected = true

	go func() {
		s.handleReconnect()
	}()

	return nil
}

func (s *Session) connect(ctx context.Context) error {
	errCh := make(chan error)

	var (
		conn *amqp.Connection
		err  error
	)

	go func() {
		conn, err = amqp.Dial(s.cfg.AMQP.URI)
		errCh <- err
	}()

	select {
	case <-ctx.Done():
		return ErrConnectionTimeout
	case <-errCh:
	}

	if err != nil {
		return err
	}

	s.conn = conn
	s.reconn = s.conn.NotifyClose(make(chan *amqp.Error))

	return nil
}

func (s *Session) handleReconnect() error {

	var err error
	for {
		select {
		case <-s.done:
			return nil
		case err = <-s.reconn:
			s.connected = false
		}

		done := (<-chan time.Time)(make(chan time.Time))
		ctx := context.Background()
		cancel := context.CancelFunc(func() {})

		if s.cfg.ConnectionTimeout != 0 {
			done = time.After(s.cfg.ConnectionTimeout)
			ctx, cancel = context.WithTimeout(context.Background(), s.cfg.ConnectionTimeout)
			defer cancel()
		}

		for {
			select {
			case <-done:
				return err
			case <-time.After(100 * time.Millisecond):
			}
			if err = s.connect(ctx); err == nil {
				break
			}
		}

		cancel()

		s.connected = true
	}
}

type PriorityType string

var (
	PriorityTypeHigh PriorityType = "High"
	PriorityTypeLow  PriorityType = "Low"
)

// Task represents a task
type Task struct {
	Name    string
	Payload []byte

	Priority PriorityType
	id       string
}

// ID returns the task id
func (t *Task) ID() string {
	return t.id
}

// List of available errors
var (
	ErrNotConnected      = errors.New("cogman: client not connected")
	ErrNotPublished      = errors.New("cogman: task not published")
	ErrInvalidConfig     = errors.New("cogman: invalid client config")
	ErrRequestTimeout    = errors.New("cogman: request timeout")
	ErrConnectionTimeout = errors.New("cogman: connection timeout")
)

// SendTask sends task t
func (s *Session) SendTask(t *Task) error {
	s.mu.RLock()
	if !s.connected {
		return ErrNotConnected
	}
	s.mu.RUnlock()

	ch, err := s.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	if err := ch.Confirm(false); err != nil {
		return err
	}

	id := uuid.New().String()

	close := ch.NotifyClose(make(chan *amqp.Error))
	publish := ch.NotifyPublish(make(chan amqp.Confirmation))

	Queue := s.GetQueueName(t.Priority)
	errs := make(chan error)

	go func() {
		err := ch.Publish(
			s.cfg.AMQP.Exchange,
			Queue,
			false,
			false,
			amqp.Publishing{
				Headers: map[string]interface{}{
					"TaskName": t.Name,
				},
				Type:         t.Name,
				MessageId:    id,
				DeliveryMode: amqp.Persistent,
				Body:         t.Payload,
			},
		)
		if err != nil {
			errs <- err
		}
	}()

	done := (<-chan time.Time)(make(chan time.Time))
	if s.cfg.RequestTimeout != 0 {
		done = time.After(s.cfg.RequestTimeout)
	}

	select {
	case err := <-close:
		return err
	case err := <-errs:
		return err
	case p := <-publish:
		if !p.Ack {
			return ErrNotPublished
		}
	case <-done:
		return ErrRequestTimeout
	}

	t.id = id

	return nil
}

func (s *Session) GetQueueName(pType PriorityType) string {
	queueType := ""
	name := ""

	if pType == PriorityTypeHigh {
		queueType = "priority_queue"
	} else {
		queueType = "lazy_queue"
	}
	for {
		queue := fmt.Sprintf("%s_%d", queueType, s.getQueueIndex())
		if _, err := s.EnsureQueue(s.conn, queue); err == nil {
			name = queue
			break
		}
	}

	return name
}

func (s *Session) getQueueIndex() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	index := s.queueIndex
	s.queueIndex++
	s.queueIndex = s.queueIndex % s.cfg.AMQP.PriorityQueueCount

	return index
}

func (s *Session) EnsureQueue(con *amqp.Connection, queue string) (*amqp.Queue, error) {
	chnl, err := con.Channel()
	if err != nil {
		return nil, err
	}

	defer chnl.Close()

	qu, err := chnl.QueueDeclare(
		queue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return &qu, nil
}
