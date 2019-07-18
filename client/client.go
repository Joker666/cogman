package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Tapfury/cogman/config"
	"github.com/Tapfury/cogman/infra"
	"github.com/Tapfury/cogman/util"

	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

// Session holds necessery fields of a client session
type Session struct {
	cfg *config.Client

	mu        sync.RWMutex
	connected bool

	conn      *amqp.Connection
	redisConn *infra.RedisClient

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
		redisConn:  infra.NewRedisClient(cfg.Redis.URI),
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

	if err := s.redisConn.Ping(); err != nil {
		return err
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

// SendTask sends task t
func (s *Session) SendTask(t *util.Task) error {
	s.mu.RLock()
	if !s.connected {
		return ErrNotConnected
	}
	s.mu.RUnlock()

	if !t.Priority.Valid() {
		return ErrInvalidPriority
	}

	ch, err := s.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	if err := ch.Confirm(false); err != nil {
		return err
	}

	t.ID = uuid.New().String()

	close := ch.NotifyClose(make(chan *amqp.Error))
	publish := ch.NotifyPublish(make(chan amqp.Confirmation))

	Queue := s.GetQueueName(t.Priority)
	errs := make(chan error)

	go func() {
		s.redisConn.CreateTask(t)
		err := ch.Publish(
			s.cfg.AMQP.Exchange,
			Queue,
			false,
			false,
			amqp.Publishing{
				Headers: map[string]interface{}{
					"TaskName": t.Name,
					"TaskID":   t.ID,
				},
				Type:         t.Name,
				DeliveryMode: amqp.Persistent,
				Body:         t.Payload,
			},
		)
		if err != nil {
			errs <- err
			s.redisConn.UpdateTaskStatus(t.ID, util.StatusFailed, err)
			return
		}

		s.redisConn.UpdateTaskStatus(t.ID, util.StatusQueued, nil)
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

	return nil
}

func (s *Session) GetQueueName(pType util.PriorityType) string {
	queueType := util.LowPriorityQueue
	name := ""

	if pType == util.PriorityTypeHigh {
		queueType = util.HighPriorityQueue
	}

	for {
		// TODO: Handle low priority queue
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
	s.queueIndex = s.queueIndex % s.cfg.AMQP.HighPriorityQueueCount

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
