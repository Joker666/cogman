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
	taskRep repo.Task
	acon    *amqp.Connection

	workers map[string]*worker

	lgr util.Logger

	quit, done chan struct{}
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
		s.acon.Close()
		s.running = false
	}()

	// TODO: Handle low priority queue
	s.lgr.Debug("ensuring queue: ")
	for i := 0; i < s.cfg.AMQP.HighPriorityQueueCount; i++ {
		queue := getQueueName(util.HighPriorityQueue, i)
		if err := ensureQueue(s.acon, queue); err != nil {
			s.lgr.Error("failed to ensure queue: "+queue, err)
			return err
		}

		s.lgr.Debug(queue + " ensured")
	}

	ctx, stop := context.WithCancel(context.Background())

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		// TODO: handle error
		s.consume(ctx, s.cfg.AMQP.Prefetch)
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

	if s.cfg.Mongo.URI != "" {
		s.lgr.Debug("connecting mongodb", util.Object{"uri", s.cfg.Mongo.URI})
		mcl, err := infra.NewMongoClient(s.cfg.Mongo.URI)
		if err != nil {
			return err
		}

		s.lgr.Debug("pinging mongodb", util.Object{"uri", s.cfg.Mongo.URI})
		if err := mcl.Ping(); err != nil {
			return err
		}

		s.taskRep.MongoConn = mcl
	}

	s.lgr.Debug("dialing amqp", util.Object{"uri", s.cfg.AMQP.URI})
	acl, err := amqp.Dial(s.cfg.AMQP.URI)
	if err != nil {
		s.lgr.Error("failed amqp dial", err)
		return err
	}
	s.acon = acl

	rcon := infra.NewRedisClient(s.cfg.Redis.URI)
	s.lgr.Debug("pinging redis", util.Object{"uri", s.cfg.Redis.URI})
	if err := rcon.Ping(); err != nil {
		s.lgr.Error("failed redis ping", err)
	}
	s.taskRep.RedisConn = rcon

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

type errorTaskBody struct {
	taskID string
	status util.Status
	err    error
}

func (s *Server) consume(ctx context.Context, prefetch int) error {
	errCh := make(chan errorTaskBody, 1)

	s.lgr.Debug("creating channel")

	chnl, err := s.acon.Channel()
	if err != nil {
		s.lgr.Error("failed to create channel", err)
		return err
	}

	defer chnl.Close()

	s.lgr.Debug("setting channel qos")
	if err := chnl.Qos(prefetch, 0, false); err != nil {
		s.lgr.Error("failed to set qos", err)
		return err
	}

	taskPool := make(chan amqp.Delivery)
	close := chnl.NotifyClose(make(chan *amqp.Error, 1))

	// TODO: Handle low priority queue
	s.lgr.Debug("creating consumer")
	for i := 0; i < s.cfg.AMQP.HighPriorityQueueCount; i++ {
		queue := getQueueName(util.HighPriorityQueue, i)

		go func() {
			msg, err := chnl.Consume(
				queue,
				"",
				true,
				false,
				false,
				false,
				nil,
			)
			if err != nil {
				s.lgr.Error("failed to create consumer", err)
				return
			}

			for {
				select {
				case <-ctx.Done():
					s.lgr.Debug("queue closing", util.Object{"queue", queue})
					return
				case taskPool <- (<-msg):
					s.lgr.Debug("new task", util.Object{"queue", queue})
				}
			}
		}()
	}

	wg := sync.WaitGroup{}

	for {
		var msg amqp.Delivery

		done := false

		select {
		case closeErr := <-close:
			s.lgr.Error("Server closed", closeErr)
			done = true
		case <-ctx.Done():
			s.lgr.Debug("task processing stopped")
			done = true
		case msg = <-taskPool:
			s.lgr.Debug("received a task to process", util.Object{"msgID", msg.MessageId})
		case err := <-errCh:
			s.lgr.Error("got error in task", err.err, util.Object{"ID", err.taskID})
			s.taskRep.UpdateTaskStatus(err.taskID, err.status, err.err)
			continue
		}

		if done {
			break
		}

		// TODO: retry task if fail. value will be send by header

		hdr := msg.Headers
		if hdr == nil {
			s.lgr.Warn("skipping headless task")
			continue
		}

		taskID, ok := hdr["TaskID"].(string)
		if !ok {
			s.lgr.Warn("skipping unidentified task")
			continue
		}

		s.taskRep.UpdateTaskStatus(taskID, util.StatusInProgress)

		taskName, ok := hdr["TaskName"].(string)
		if !ok {
			errCh <- errorTaskBody{
				taskID,
				util.StatusFailed,
				ErrTaskUnidentified,
			}
			continue
		}

		wrkr, ok := s.workers[taskName]
		if !ok {
			errCh <- errorTaskBody{
				taskID,
				util.StatusFailed,
				ErrTaskUnhandled,
			}
			continue
		}

		wg.Add(1)
		go func(wrkr *worker, msg *amqp.Delivery) {
			defer wg.Done()

			s.lgr.Info("processing task", util.Object{"taskName", wrkr.taskName}, util.Object{"taskID", taskID})
			startAt := time.Now()
			if err := wrkr.process(msg); err != nil {
				errCh <- errorTaskBody{
					taskID,
					util.StatusFailed,
					err,
				}
				return
			}
			duration := float64(time.Since(startAt)) / float64(time.Minute)

			s.taskRep.UpdateTaskStatus(taskID, util.StatusSuccess, duration)

		}(wrkr, &msg)
	}

	wg.Wait()

	return err
}

func getQueueName(prefix string, id int) string {
	return prefix + "_" + strconv.Itoa(id)
}
