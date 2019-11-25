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

// Session holds necessary fields of a client session
type Session struct {
	cfg *config.Client

	mu        sync.RWMutex
	connected bool

	conn     *amqp.Connection
	taskRepo *repo.TaskRepository

	done   chan struct{}
	reconn chan *amqp.Error

	lgr        util.Logger
	queueIndex map[string]int
}

// NewSession creates new client session with config cfg
func NewSession(cfg config.Client) (*Session, error) {
	if cfg.ConnectionTimeout < 0 || cfg.RequestTimeout < 0 ||
		cfg.Mongo.TTL < 0 || cfg.Redis.TTL < 0 {
		return nil, ErrInvalidConfig
	}

	if cfg.Mongo.TTL == 0 {
		cfg.Mongo.TTL = time.Hour * 24 * 30 // 1  month
	}

	if cfg.Redis.TTL == 0 {
		cfg.Redis.TTL = time.Hour * 24 * 7 // 1 week
	}

	return &Session{
		cfg: &cfg,
		lgr: util.NewLogger(),
		queueIndex: map[string]int{
			util.QueueModeLazy:    0,
			util.QueueModeDefault: 0,
		},

		taskRepo: &repo.TaskRepository{},
	}, nil
}

// Close closes session s. It must be defer from the method client initiated.
func (s *Session) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.connected = false
	s.taskRepo.CloseClients()

	close(s.done)

	return s.conn.Close()
}

// Connect connects a client session. It also take care reconnection process.
// For result log, Redis & Mongo connection will be initiated.
// Mongo indices ensured here. If any conflict occurred, drop the indice table.
func (s *Session) Connect() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// block connection overriding
	if s.connected {
		return nil
	}

	// redis connection
	rcon := infra.NewRedisClient(s.cfg.Redis.URI, s.cfg.Redis.TTL)
	if err := rcon.Ping(); err != nil {
		return err
	}

	// mongo connection
	var mcon *infra.MongoClient
	if s.cfg.Mongo.URI != "" {
		con, err := infra.NewMongoClient(s.cfg.Mongo.URI, s.cfg.Mongo.TTL)
		if err != nil {
			return err
		}

		if err := con.Ping(); err != nil {
			return err
		}

		mcon = con
		_, err = mcon.SetTTL()
		if err != nil {
			return err
		}
	}

	s.taskRepo = repo.NewTaskRepo(rcon, mcon)
	if err := s.taskRepo.EnsureIndices(); err != nil {
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

	// handle reconnection
	go s.handleReconnect()

	// Re-enqueue
	s.reEnqueue()

	return nil
}

// reEnqueue initiated task from connection lost period.
func (s *Session) reEnqueue() {
	// checking client side permission client side
	if !s.cfg.ReEnqueue {
		return
	}

	// mongo connection required
	if s.taskRepo.MongoConn == nil {
		s.lgr.Warn("Failed to re-enqueue task. Mongo connection missing")
		return
	}

	nw := time.Now()
	go func() {
		if err := s.reEnqueueUnhandledTasksBefore(nw); err != nil {
			s.lgr.Error("Error in re-enqueuing: ", err)
		}
	}()
}

// connection initiate a cogman client above rabbitmq
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

func (s *Session) handleReconnect() {
	for {
		select {
		case <-s.done:
			s.lgr.Info("Cogman Client: Connection closing.")
			return
		case err := <-s.reconn:
			s.lgr.Error("Cogman Client: Connection reconnecting.", err)
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
				s.lgr.Warn("Cogman Client: failed to reconnect. client closing")
				return
			case <-time.After(100 * time.Millisecond):
			}
			if err := s.connect(ctx); err == nil {
				// Re enqueue un handles tasks
				s.reEnqueue()
				break
			}
		}

		cancel()

		s.connected = true
	}
}
