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

	cfg       *config.Server
	taskRep   *repo.Task
	acon      *amqp.Connection
	retryConn *client.Session

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

func newRetryClient(cfg *config.Server) (*client.Session, error) {
	clntCfg := config.Client{
		ConnectionTimeout: time.Minute * 5, // Need to change
		RequestTimeout:    time.Second * 5, // Need to change

		AMQP:  cfg.AMQP,
		Mongo: cfg.Mongo,
		Redis: cfg.Redis,
	}

	return client.NewSession(clntCfg)
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
		s.retryConn.Close()
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

	s.lgr.Debug("dialing amqp", util.Object{Key: "uri", Val: s.cfg.AMQP.URI})
	acl, err := amqp.Dial(s.cfg.AMQP.URI)
	if err != nil {
		s.lgr.Error("failed amqp dial", err)
		return err
	}
	s.acon = acl

	rcon := infra.NewRedisClient(s.cfg.Redis.URI)
	s.lgr.Debug("pinging redis", util.Object{Key: "uri", Val: s.cfg.Redis.URI})
	if err := rcon.Ping(); err != nil {
		s.lgr.Error("failed redis ping", err)
	}

	s.taskRep = repo.NewTaskRepo(rcon, mcl)

	retryConn, err := newRetryClient(s.cfg)
	if err != nil {
		return err
	}
	s.lgr.Debug("retry session stablished")
	s.retryConn = retryConn

	if err := s.retryConn.Connect(); err != nil {
		return err
	}
	s.lgr.Debug("retry session connected")

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
	closeNotification := chnl.NotifyClose(make(chan *amqp.Error, 1))

	s.lgr.Debug("creating consumer")
	for i := 0; i < s.cfg.AMQP.HighPriorityQueueCount; i++ {
		queue := formQueueName(util.HighPriorityQueue, i)
		s.setConsumer(ctx, chnl, queue, util.QueueModeDefault, taskPool)
	}

	for i := 0; i < s.cfg.AMQP.LowPriorityQueueCount; i++ {
		queue := formQueueName(util.LowPriorityQueue, i)
		s.setConsumer(ctx, chnl, queue, util.QueueModeLazy, taskPool)
	}

	wg := sync.WaitGroup{}

	for {
		var msg amqp.Delivery

		done := false

		select {
		case closeErr := <-closeNotification:
			s.lgr.Error("Server closed", closeErr)
			done = true
		case <-ctx.Done():
			s.lgr.Debug("task processing stopped")
			done = true
		case msg = <-taskPool:
			s.lgr.Debug("received a task to process", util.Object{Key: "msgID", Val: msg.MessageId})
		case errTask := <-errCh:
			s.lgr.Error("got error in task", errTask.err, util.Object{Key: "ID", Val: errTask.taskID})
			func() {
				task, err := s.taskRep.GetTask(errTask.taskID)
				if err != nil {
					s.lgr.Error("failed to get task", err, util.Object{Key: "TaskID", Val: errTask.taskID})
					return
				}

				if orgTask, err := s.taskRep.GetTask(task.OriginalTaskID); err != nil {
					s.lgr.Error("failed to get task", err, util.Object{Key: "TaskID", Val: orgTask.TaskID})
					return
				} else if orgTask.Retry != 0 {
					go func() {
						s.retryConn.RetryTask(*orgTask)
					}()
				}
			}()

			s.taskRep.UpdateTaskStatus(errTask.taskID, errTask.status, errTask.err)
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

			s.lgr.Info("processing task", util.Object{Key: "taskName", Val: wrkr.taskName}, util.Object{Key: "taskID", Val: taskID})
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

func (s *Server) setConsumer(ctx context.Context, chnl *amqp.Channel, queue string, mode string, taskPool chan<- amqp.Delivery) {
	go func() {
		msg, err := chnl.Consume(
			queue,
			"",
			true,
			false,
			false,
			false,
			amqp.Table{
				"x-queue-mode": mode,
			},
		)
		if err != nil {
			s.lgr.Error("failed to create consumer", err)
			return
		}

		for {
			select {
			case <-ctx.Done():
				s.lgr.Debug("queue closing", util.Object{Key: "queue", Val: queue})
				return
			case taskPool <- <-msg:
				s.lgr.Debug("new task", util.Object{Key: "queue", Val: queue})
			}
		}
	}()
}

func formQueueName(prefix string, id int) string {
	return prefix + "_" + strconv.Itoa(id)
}
