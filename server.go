package cogman

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/Tapfury/cogman/client"
	"github.com/Tapfury/cogman/config"
	"github.com/Tapfury/cogman/infra"
	"github.com/Tapfury/cogman/repo"
	"github.com/Tapfury/cogman/rest"
	"github.com/Tapfury/cogman/util"

	"github.com/streadway/amqp"
)

type Server struct {
	tmu sync.RWMutex
	mu  sync.Mutex

	running bool

	cfg       *config.Server
	taskRepo  *repo.TaskRepository
	acon      *amqp.Connection
	retryConn *client.Session

	workers map[string]*util.Worker

	lgr util.Logger

	quit, done chan struct{}

	connError chan error
}

func NewServer(cfg config.Server) (*Server, error) {
	if cfg.ConnectionTimeout < 0 {
		return nil, ErrInvalidConfig
	}

	srvr := &Server{
		cfg:       &cfg,
		quit:      make(chan struct{}),
		done:      make(chan struct{}),
		connError: make(chan error, 1),

		workers: map[string]*util.Worker{},
		lgr:     util.NewLogger(),

		taskRepo: &repo.TaskRepository{},
	}

	return srvr, nil
}

func newRetryClient(cfg *config.Server) (*client.Session, error) {
	clntCfg := config.Client{
		ConnectionTimeout: cfg.ConnectionTimeout,
		RequestTimeout:    time.Second * 5, // TODO: need to update

		AMQP:  cfg.AMQP,
		Mongo: cfg.Mongo,
		Redis: cfg.Redis,

		ReEnqueue: false,
	}

	return client.NewSession(clntCfg)
}

func (s *Server) Register(taskName string, h util.Handler) error {
	s.lgr.Debug("registering task ", util.Object{Key: "TaskName", Val: taskName})

	s.tmu.Lock()
	defer s.tmu.Unlock()

	if _, ok := s.workers[taskName]; ok {
		s.lgr.Error("duplicate task ", ErrDuplicateTaskName, util.Object{Key: "TaskName", Val: taskName})
		return ErrDuplicateTaskName
	}

	s.workers[taskName] = util.NewWorker(taskName, h)

	s.lgr.Info("registered task ", util.Object{Key: "TaskName", Val: taskName})
	return nil
}

func (s *Server) GetTaskHandler(taskName string) util.Handler {
	s.tmu.RLock()
	defer s.tmu.RUnlock()

	s.lgr.Debug("getting task ", util.Object{Key: "TaskName", Val: taskName})
	return s.workers[taskName].Handler()
}

func (s *Server) Start() error {
	s.mu.Lock()

	s.lgr.Debug("starting server")

	if s.running {
		s.mu.Unlock()
		s.lgr.Error("server already running", ErrRunningServer)
		return ErrRunningServer
	}

	s.lgr.Debug("bootstraping server")
	if err := s.bootstrap(); err != nil {
		s.mu.Unlock()
		s.lgr.Error("failed to bootstrap", err)
		return err
	}

	s.running = true
	s.mu.Unlock()

	defer func() {
		s.lgr.Debug("closing connections")
		s.taskRepo.CloseClients()
		_ = s.retryConn.Close()
		_ = s.acon.Close()
		s.running = false
	}()

	s.lgr.Debug("ensuring queue")
	for i := 0; i < s.cfg.AMQP.HighPriorityQueueCount; i++ {
		queue := formQueueName(util.HighPriorityQueue, i)
		if err := ensureQueue(s.acon, queue, util.TaskPriorityHigh); err != nil {
			s.lgr.Error("failed to ensure queue", err, util.Object{Key: "queue_name", Val: queue})
			return err
		}

		s.lgr.Debug(queue + " ensured")
	}

	for i := 0; i < s.cfg.AMQP.LowPriorityQueueCount; i++ {
		queue := formQueueName(util.LowPriorityQueue, i)
		if err := ensureQueue(s.acon, queue, util.TaskPriorityLow); err != nil {
			s.lgr.Error("failed to ensure queue", err, util.Object{Key: "queue_name", Val: queue})
			return err
		}

		s.lgr.Debug(queue + " ensured")
	}

	ctx, stop := context.WithCancel(context.Background())

	go rest.StartRestServer(ctx, "", s.taskRepo, s.lgr)
	s.lgr.Info("rest server started", util.Object{Key: "port", Val: "8081"})

	go s.Consume(ctx, s.cfg.AMQP.Prefetch)

	s.lgr.Info("server started")

	go func() {
		_ = s.handleReconnect(ctx)
	}()

	<-s.quit

	s.lgr.Debug("found stop signal")

	stop() // stopping reConnection handler & consumer

	s.done <- struct{}{}

	return nil
}

func ensureQueue(con *amqp.Connection, queue string, taskType util.TaskPriority) error {
	chnl, err := con.Channel()
	if err != nil {
		return err
	}

	mode := util.QueueModeLazy
	if taskType == util.TaskPriorityHigh {
		mode = util.QueueModeDefault
	}

	_, err = chnl.QueueDeclare(
		queue,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-queue-mode": mode,
		},
	)
	if err != nil {
		return err
	}

	return chnl.Close()
}

func (s *Server) bootstrap() error {
	var mcl *infra.MongoClient
	if s.cfg.Mongo.URI != "" {
		s.lgr.Debug("connecting mongodb", util.Object{Key: "uri", Val: s.cfg.Mongo.URI})
		con, err := infra.NewMongoClient(s.cfg.Mongo.URI, s.cfg.Mongo.TTL)
		if err != nil {
			return err
		}

		s.lgr.Debug("pinging mongodb", util.Object{Key: "uri", Val: s.cfg.Mongo.URI})
		if err := con.Ping(); err != nil {
			return err
		}

		mcl = con
		_, err = mcl.SetTTL()
		if err != nil {
			return err
		}
	}

	rcon := infra.NewRedisClient(s.cfg.Redis.URI, s.cfg.Redis.TTL)
	s.lgr.Debug("pinging redis", util.Object{Key: "uri", Val: s.cfg.Redis.URI})
	if err := rcon.Ping(); err != nil {
		s.lgr.Error("failed redis ping", err)
	}

	s.taskRepo = repo.NewTaskRepo(rcon, mcl)

	ctx := context.Background()
	cancel := context.CancelFunc(func() {})

	if s.cfg.ConnectionTimeout != 0 {
		ctx, cancel = context.WithTimeout(context.Background(), s.cfg.ConnectionTimeout)
		defer cancel()
	}

	s.lgr.Debug("dialing amqp", util.Object{Key: "uri", Val: s.cfg.AMQP.URI})
	if err := s.connect(ctx); err != nil {
		s.lgr.Error("failed amqp dial", err)
		return err
	}

	s.lgr.Info("getting retry session")
	retryConn, err := newRetryClient(s.cfg)
	if err != nil {
		s.lgr.Error("failed get retry session", err)
		return err
	}
	s.lgr.Debug("retry session established")
	s.retryConn = retryConn

	if err := s.retryConn.Connect(); err != nil {
		s.lgr.Error("failed connect retry session", err)
		return err
	}
	s.lgr.Debug("retry session connected")

	return nil
}

func (s *Server) connect(ctx context.Context) error {
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

	s.acon = conn

	return nil
}

func (s *Server) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.lgr.Debug("stopping server")

	if !s.running {
		s.lgr.Error("server already stopped", ErrStoppedServer)
		return ErrStoppedServer
	}

	s.quit <- struct{}{}
	<-s.done

	s.lgr.Info("server stopped")

	return nil
}

func (s *Server) handleReconnect(ctx context.Context) error {
	var err error
	for {
		select {
		case <-ctx.Done():
			s.lgr.Warn("Retry stopped")
			return nil
		case err = <-s.connError:
			s.lgr.Error("Error in consumer", err)
		}

		s.lgr.Info("Trying to reconnect")
		s.running = false

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
				go s.Consume(ctx, s.cfg.AMQP.Prefetch)
				break
			}
		}

		cancel()

		s.lgr.Info("Reconnection successful")
		s.running = true
	}
}

func formQueueName(prefix string, id int) string {
	return prefix + "_" + strconv.Itoa(id)
}
