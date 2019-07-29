package client

import (
	"context"
	"sync"
	"time"

	"github.com/Tapfury/cogman/config"
	"github.com/Tapfury/cogman/infra"
	"github.com/Tapfury/cogman/repo"
	"github.com/Tapfury/cogman/util"

	"github.com/streadway/amqp"
)

// Session holds necessery fields of a client session
type Session struct {
	cfg *config.Client

	mu        sync.RWMutex
	connected bool

	conn     *amqp.Connection
	taskRepo *repo.Task

	done   chan struct{}
	reconn chan *amqp.Error

	lgr        util.Logger
	queueIndex map[string]int
}

// NewSession creates new client session with config cfg
func NewSession(cfg config.Client) (*Session, error) {
	if cfg.ConnectionTimeout < 0 || cfg.RequestTimeout < 0 {
		return nil, ErrInvalidConfig
	}

	return &Session{
		cfg: &cfg,
		lgr: util.NewLogger(),
		queueIndex: map[string]int{
			util.QueueModeLazy:    0,
			util.QueueModeDefault: 0,
		},
	}, nil
}

// Close closes session s
func (s *Session) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.connected = false
	s.taskRepo.CloseClients()

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

	rcon := infra.NewRedisClient(s.cfg.Redis.URI)
	if err := rcon.Ping(); err != nil {
		return err
	}

	var mcon *infra.MongoClient
	if s.cfg.Mongo.URI != "" {
		con, err := infra.NewMongoClient(s.cfg.Mongo.URI)
		if err != nil {
			return err
		}

		if err := con.Ping(); err != nil {
			return err
		}

		mcon = con
	}

	s.taskRepo = repo.NewTaskRepo(rcon, mcon)

	s.taskRepo.SetLogger()

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

	if s.taskRepo.MongoConn != nil {
		nw := time.Now()
		go func() {
			if err := s.ReEnqueueUnhandledTasksBefore(nw); err != nil {
				s.lgr.Error("Error in re-enqueuing: ", err)
			}
		}()
	}

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
