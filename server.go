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
	tmu   sync.RWMutex

	mu      sync.Mutex
	running bool

	cfg  *config.Server
	mcon *mongo.Client
	rcon *redis.Pool
	acon *amqp.Connection

	quit, done chan struct{}
}

func NewServer(cfg config.Server) (*Server, error) {
	return &Server{
		cfg:  &cfg,
		quit: make(chan struct{}),
		done: make(chan struct{}),
	}, nil
}

func (s *Server) Register(t Task, h Handler) error {
	s.tmu.Lock()
	defer s.tmu.Unlock()

	name := t.Name()
	if _, ok := s.tasks[name]; ok {
		return ErrDuplicateTaskName
	}
	s.tasks[name] = h
	return nil
}

func (s *Server) GetTaskHandler(t Task) Handler {
	s.tmu.RLock()
	defer s.tmu.RUnlock()

	return s.tasks[t.Name()]
}

func (s *Server) Start() error {
	s.mu.Lock()

	if s.running {
		s.mu.Unlock()
		return ErrRunningServer
	}

	if err := s.bootstrap(); err != nil {
		s.mu.Unlock()
		return err
	}

	s.running = true
	s.mu.Unlock()

	defer func() {
		s.mcon.Disconnect(context.Background())
		s.rcon.Close()
		s.acon.Close()
		s.running = false
	}()

	ctx, stop := context.WithCancel(context.Background())

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		s.consume(ctx)
		wg.Done()
	}()

	<-s.quit
	stop()

	wg.Wait()
	s.done <- struct{}{}

	return nil
}

func (s *Server) bootstrap() error {
	mcl, err := mongo.Connect(context.Background(), options.Client().ApplyURI(s.cfg.Mongo.URI))
	if err != nil {
		return err
	}

	if err := mcl.Ping(context.Background(), readpref.Primary()); err != nil {
		return err
	}

	acl, err := amqp.Dial(s.cfg.AMQP.URI)
	if err != nil {
		return err
	}
	if err := acl.Close(); err != nil {
		return err
	}

	rpol := &redis.Pool{
		MaxIdle:     5,
		IdleTimeout: 300 * time.Second,
		Dial: func() (redis.Conn, error) {
			return redis.DialURL(s.cfg.Redis.URI)
		},
	}

	rcon := rpol.Get()
	defer rcon.Close()

	if _, err := rcon.Do("PING"); err != nil {
		return err
	}

	s.mcon = mcl
	s.rcon = rpol
	s.acon = acl

	return nil
}

func (s *Server) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return ErrStoppedServer
	}

	s.quit <- struct{}{}
	<-s.done

	return nil
}

func (s *Server) consume(ctx context.Context) {

}
