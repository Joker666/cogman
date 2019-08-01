package cogman

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/Tapfury/cogman/config"
	"github.com/Tapfury/cogman/infra"
	"github.com/Tapfury/cogman/repo"
	"github.com/Tapfury/cogman/util"

	"github.com/streadway/amqp"
)

type Handler interface {
	Do(ctx context.Context, payload []byte) error
}

type Server struct {
	tasks map[string]Handler
	tmu   sync.RWMutex

	mu      sync.Mutex
	running bool

	cfg     *config.Server
	taskRep *repo.Task
	acon    *amqp.Connection

	workers map[string]*worker

	lgr util.Logger

	quit, done chan struct{}
	reconn     chan *amqp.Error
}

func NewServer(cfg config.Server) (*Server, error) {
	if len(cfg.Tasks) == 0 {
		return nil, ErrNoTask
	}

	srvr := &Server{
		cfg:  &cfg,
		quit: make(chan struct{}),
		done: make(chan struct{}),

		tasks:   map[string]Handler{},
		workers: map[string]*worker{},
		lgr:     util.NewLogger(),

		taskRep: &repo.Task{},
	}

	return srvr, nil
}

func (s *Server) Register(taskName string, h Handler) error {
	s.lgr.Debug("registering task " + taskName)

	s.tmu.Lock()
	defer s.tmu.Unlock()

	if _, ok := s.tasks[taskName]; ok {
		s.lgr.Error("duplicate task "+taskName, ErrDuplicateTaskName)
		return ErrDuplicateTaskName
	}
	s.tasks[taskName] = h

	s.lgr.Info("registered task " + taskName)

	return nil
}

func (s *Server) GetTaskHandler(taskName string) Handler {
	s.tmu.RLock()
	defer s.tmu.RUnlock()

	s.lgr.Debug("getting task " + taskName)
	return s.tasks[taskName]
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
		s.taskRep.CloseClients()
		_ = s.acon.Close()
		s.running = false
	}()

	s.lgr.Debug("ensuring queue: ")
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

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		// TODO: handle error
		_ = s.consume(ctx, s.cfg.AMQP.Prefetch)
		wg.Done()
	}()

	s.lgr.Info("server started")

	<-s.quit

	s.lgr.Debug("found stop signal")

	stop()

	s.lgr.Debug("waiting for completing running tasks")

	wg.Wait()
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
	s.lgr.Debug("initializing workers")
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

	var mcl *infra.MongoClient
	if s.cfg.Mongo.URI != "" {
		s.lgr.Debug("connecting mongodb", util.Object{Key: "uri", Val: s.cfg.Mongo.URI})
		con, err := infra.NewMongoClient(s.cfg.Mongo.URI)
		if err != nil {
			return err
		}

		s.lgr.Debug("pinging mongodb", util.Object{Key: "uri", Val: s.cfg.Mongo.URI})
		if err := con.Ping(); err != nil {
			return err
		}

		mcl = con
	}

	rcon := infra.NewRedisClient(s.cfg.Redis.URI)
	s.lgr.Debug("pinging redis", util.Object{Key: "uri", Val: s.cfg.Redis.URI})
	if err := rcon.Ping(); err != nil {
		s.lgr.Error("failed redis ping", err)
	}

	s.taskRep = repo.NewTaskRepo(rcon, mcl)

	s.lgr.Debug("dialing amqp", util.Object{Key: "uri", Val: s.cfg.AMQP.URI})
	if err := s.connect(); err != nil {
		s.lgr.Error("failed amqp dial", err)
		return err
	}

	return nil
}

func (s *Server) connect() error {
	acon, err := amqp.Dial(s.cfg.AMQP.URI)
	if err != nil {
		return err
	}

	s.reconn = acon.NotifyClose(make(chan *amqp.Error, 1))
	s.acon = acon

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

func (s *Server) handleReconnect() error {
	var err error
	for {
		select {
		case <-s.done:
			return nil
		case err = <-s.reconn:
			s.running = false
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

		s.running = true
	}
}

func formQueueName(prefix string, id int) string {
	return prefix + "_" + strconv.Itoa(id)
}
