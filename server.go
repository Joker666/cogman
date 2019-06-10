package cogman

import (
	"context"
	"sync"
	"time"

	"github.com/Tapfury/cogman/config"

	"github.com/gomodule/redigo/redis"
	"github.com/streadway/amqp"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type Task interface {
	Name() string
}

type Handler interface {
	Do(args ...interface{}) error
}

type Server struct {
	tasks map[string]Handler
	mu    sync.RWMutex

	cfg *config.Server
}

func NewServer(cfg config.Server) (*Server, error) {
	acl, err := amqp.Dial(cfg.AMQP.URI)
	if err != nil {
		return nil, err
	}
	if err := acl.Close(); err != nil {
		return nil, err
	}

	rcl := &redis.Pool{
		MaxIdle:     5,
		IdleTimeout: 300 * time.Second,
		Dial: func() (redis.Conn, error) {
			return redis.DialURL(cfg.Redis.URI)
		},
	}

	rcon := rcl.Get()
	defer rcon.Close()

	if _, err := rcon.Do("PING"); err != nil {
		return nil, err
	}

	mcl, err := mongo.Connect(context.Background(), options.Client().ApplyURI(cfg.Mongo.URI))
	if err != nil {
		return nil, err
	}

	if err := mcl.Ping(context.Background(), readpref.Primary()); err != nil {
		return nil, err
	}

	return &Server{
		cfg: &cfg,
	}, nil
}

func (s *Server) Register(t Task, h Handler) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	name := t.Name()
	if _, ok := s.tasks[name]; ok {
		return ErrDuplicateTaskName
	}
	s.tasks[name] = h
	return nil
}

func (s *Server) GetTaskHandler(t Task) Handler {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.tasks[t.Name()]
}
