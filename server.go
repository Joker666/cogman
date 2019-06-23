package cogman

import (
	"context"
	"encoding/json"
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
	Do(ctx context.Context, payload []byte) error
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

	workers map[string]*worker

	lgr Logger

	quit, done chan struct{}
}

type object struct {
	key string
	val interface{}
}

type entry map[string]interface{}

func prepareEntry(msg string, err error, objects ...object) entry {
	v := map[string]interface{}{
		"message": msg,
	}
	if err != nil {
		v["error"] = err
	}
	for _, o := range objects {
		v[o.key] = o.val
	}
	return v
}

func (s *Server) error(msg string, err error, objects ...object) {
	s.lgr.Log(LogLevelError, prepareEntry(msg, err, objects...))
}

func (s *Server) warn(msg string, objects ...object) {
	s.lgr.Log(LogLevelWarn, prepareEntry(msg, nil, objects...))
}

func (s *Server) info(msg string, objects ...object) {
	s.lgr.Log(LogLevelInfo, prepareEntry(msg, nil, objects...))
}

func (s *Server) debug(msg string, objects ...object) {
	s.lgr.Log(LogLevelDebug, prepareEntry(msg, nil, objects...))
}

func NewServer(cfg config.Server) (*Server, error) {
	if len(cfg.Tasks) == 0 {
		return nil, ErrNoTask
	}

	srvr := &Server{
		cfg:  &cfg,
		quit: make(chan struct{}),
		done: make(chan struct{}),

		lgr: StdLogger,
	}

	return srvr, nil
}

func (s *Server) Register(t Task, h Handler) error {
	name := t.Name()

	s.debug("registering task " + name)

	s.tmu.Lock()
	defer s.tmu.Unlock()

	if _, ok := s.tasks[name]; ok {
		s.error("duplicate task "+name, ErrDuplicateTaskName)
		return ErrDuplicateTaskName
	}
	s.tasks[name] = h

	s.info("registered task " + name)

	return nil
}

func (s *Server) GetTaskHandler(t Task) Handler {
	s.tmu.RLock()
	defer s.tmu.RUnlock()

	name := t.Name()
	s.debug("getting task " + name)
	return s.tasks[name]
}

func (s *Server) Start() error {
	s.mu.Lock()

	s.debug("starting server")

	if s.running {
		s.mu.Unlock()
		s.error("server already running", ErrRunningServer)
		return ErrRunningServer
	}

	s.debug("bootstraping server")
	if err := s.bootstrap(); err != nil {
		s.mu.Unlock()
		s.error("failed to bootstrap", err)
		return err
	}

	s.running = true
	s.mu.Unlock()

	defer func() {
		s.debug("closing connections")
		s.mcon.Disconnect(context.Background())
		s.rcon.Close()
		s.acon.Close()
		s.running = false
	}()

	s.debug("ensuring queue: " + s.cfg.AMQP.Queue)
	if err := ensureQueue(s.acon, s.cfg.AMQP.Queue); err != nil {
		s.error("failed to ensure queue: "+s.cfg.AMQP.Queue, err)
		return err
	}

	ctx, stop := context.WithCancel(context.Background())

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		// TODO: handle error
		s.consume(ctx, s.cfg.AMQP.Queue, s.cfg.AMQP.Prefetch)
		wg.Done()
	}()

	s.info("server started")

	<-s.quit

	s.debug("found stop signal")

	stop()

	s.debug("waiting for completing running tasks")

	wg.Wait()
	s.done <- struct{}{}

	return nil
}

func ensureQueue(con *amqp.Connection, queue string) error {
	chnl, err := con.Channel()
	if err != nil {
		return err
	}

	_, err = chnl.QueueDeclare(
		queue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	return chnl.Close()
}

func (s *Server) bootstrap() error {
	s.debug("initializing workers")
	for _, t := range s.cfg.Tasks {
		h, ok := s.tasks[t.Name]
		if !ok {
			return TaskHandlerMissingError(t.Name)
		}
		wrkr := &worker{
			taskName: t.Name,
			handler:  h,
		}

		s.workers[t.Name] = wrkr
	}

	s.debug("connecting mongodb", object{"uri", s.cfg.Mongo.URI})
	mcl, err := mongo.Connect(context.Background(), options.Client().ApplyURI(s.cfg.Mongo.URI))
	if err != nil {
		s.error("failed to connect mongodb", err)
		return err
	}

	s.debug("pinging mongodb")
	if err := mcl.Ping(context.Background(), readpref.Primary()); err != nil {
		s.error("failed mongodb ping", err)
		return err
	}

	s.debug("dialing amqp", object{"uri", s.cfg.AMQP.URI})
	acl, err := amqp.Dial(s.cfg.AMQP.URI)
	if err != nil {
		s.error("failed amqp dial", err)
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

	s.debug("pinging redis", object{"uri", s.cfg.Redis.URI})
	if _, err := rcon.Do("PING"); err != nil {
		s.error("failed redis ping", err)
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

	s.debug("stopping server")

	if !s.running {
		s.error("server already stopped", ErrStoppedServer)
		return ErrStoppedServer
	}

	s.quit <- struct{}{}
	<-s.done

	s.info("server stopped")

	return nil
}

func (s *Server) consume(ctx context.Context, queue string, prefetch int) error {
	errCh := make(chan error)

	s.debug("creating channel")

	chnl, err := s.acon.Channel()
	if err != nil {
		s.error("failed to create channel", err)
		return err
	}

	defer chnl.Close()

	s.debug("setting channel qos")
	if err := chnl.Qos(prefetch, 0, false); err != nil {
		s.error("failed to set qos", err)
		return err
	}

	s.debug("creating consumer", object{"queue", queue})
	msgs, err := chnl.Consume(
		queue,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		s.error("failed to create consumer", err)
		return err
	}

	// TODO: Handle error channel

	wg := sync.WaitGroup{}

	for {
		var msg amqp.Delivery

		select {
		case <-ctx.Done():
			s.debug("got done signal")
			break
		case err = <-errCh:
			s.debug("got error in channel")
			break
		case msg = <-msgs:
			s.debug("got new task")
		}

		hdr := msg.Headers
		if hdr == nil {
			s.warn("skipping headless task")
			if err := msg.Ack(false); err != nil {
				s.error("failed to ack", err)
				errCh <- err
			}
			continue
		}

		taskName, ok := hdr["TaskName"].(string)
		if !ok {
			s.warn("skipping unidentified task")
			if err := msg.Ack(false); err != nil {
				s.error("failed to ack", err)
				errCh <- err
			}
			continue
		}

		wrkr, ok := s.workers[taskName]
		if !ok {
			s.warn("skipping unhandled task", object{"task", taskName})
			if err := msg.Ack(false); err != nil {
				s.error("failed to ack", err)
				errCh <- err
			}
			continue
		}

		wg.Add(1)
		go func(wrkr *worker, msg *amqp.Delivery) {
			s.info("processing task", object{"task", wrkr.taskName})
			if err := wrkr.process(msg); err != nil {
				s.error("task failed", err)
				if err := msg.Nack(false, true); err != nil {
					s.error("failed to nack", err)
					errCh <- err
				}
			}
			wg.Done()
		}(wrkr, &msg)
	}

	wg.Wait()

	return err
}

type Status string

const (
	StatusQueued     Status = "queued"
	StatusInProgress Status = "in_progress"
	StatusFailed     Status = "failed"
	StatusSuccess    Status = "success"
)

type task struct {
	ID        string
	Name      string
	Payload   []byte
	Status    Status
	Failure   int
	CreatedAt time.Time
	UpdatedAt time.Time
}

func (s *Server) getTask(id string) (*task, error) {
	conn := s.rcon.Get()
	defer conn.Close()

	byts, err := redis.Bytes(conn.Do("GET", id))
	if err != nil {
		return nil, err
	}

	t := task{}
	if err := json.Unmarshal(byts, &t); err != nil {
		return nil, err
	}

	return &t, nil
}

func (s *Server) updateTaskStatus(id string, status Status) error {
	conn := s.rcon.Get()
	defer conn.Close()

	byts, err := redis.Bytes(conn.Do("GET", id))
	if err != nil {
		return err
	}

	t := task{}
	if err := json.Unmarshal(byts, &t); err != nil {
		return err
	}

	t.Status = status
	t.UpdatedAt = time.Now()
	if status == StatusFailed {
		t.Failure++
	}

	byts, err = json.Marshal(t)
	if err != nil {
		return err
	}

	_, err = conn.Do("SET", id, byts)
	return err
}
