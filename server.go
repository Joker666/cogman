package cogman

import (
	"context"
	"strconv"
	"sync"

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

		tasks:   map[string]Handler{},
		workers: map[string]*worker{},
		lgr:     StdLogger,
	}

	return srvr, nil
}

func (s *Server) Register(taskName string, h Handler) error {
	s.debug("registering task " + taskName)

	s.tmu.Lock()
	defer s.tmu.Unlock()

	if _, ok := s.tasks[taskName]; ok {
		s.error("duplicate task "+taskName, ErrDuplicateTaskName)
		return ErrDuplicateTaskName
	}
	s.tasks[taskName] = h

	s.info("registered task " + taskName)

	return nil
}

func (s *Server) GetTaskHandler(taskName string) Handler {
	s.tmu.RLock()
	defer s.tmu.RUnlock()

	s.debug("getting task " + taskName)
	return s.tasks[taskName]
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
		s.taskRep.Close()
		s.acon.Close()
		s.running = false
	}()

	// TODO: Handle low priority queue
	s.debug("ensuring queue: ")
	for i := 0; i < s.cfg.AMQP.HighPriorityQueueCount; i++ {
		queue := getQueueName(util.HighPriorityQueue, i)
		if err := ensureQueue(s.acon, queue); err != nil {
			s.error("failed to ensure queue: "+queue, err)
			return err
		}

		s.debug(queue + " ensured")
	}

	ctx, stop := context.WithCancel(context.Background())

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		// TODO: handle error
		s.consume(ctx, s.cfg.AMQP.Prefetch)
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
	mcl, err := infra.NewMongoClient(s.cfg.Mongo.URI)
	if err != nil {
		s.error("failed to connect mongodb", err)
		return err
	}

	s.debug("pinging mongodb")
	if err := mcl.Ping(); err != nil {
		s.error("failed mongodb ping", err)
		return err
	}

	s.debug("dialing amqp", object{"uri", s.cfg.AMQP.URI})
	acl, err := amqp.Dial(s.cfg.AMQP.URI)
	if err != nil {
		s.error("failed amqp dial", err)
		return err
	}

	rcon := infra.NewRedisClient(s.cfg.Redis.URI)
	s.debug("pinging redis", object{"uri", s.cfg.Redis.URI})
	if err := rcon.Ping(); err != nil {
		s.error("failed redis ping", err)
	}

	s.taskRep.MongoConn = mcl
	s.taskRep.RedisConn = rcon
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

type errorTaskBody struct {
	taskID string
	status util.Status
	err    error
}

func (s *Server) consume(ctx context.Context, prefetch int) error {
	defer ctx.Done()

	errCh := make(chan errorTaskBody)

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

	taskPool := make(chan amqp.Delivery)

	// TODO: Handle low priority queue
	s.debug("creating consumer")
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
				s.error("failed to create consumer", err)
				return
			}

			for {
				select {
				case <-ctx.Done():
					s.debug("queue closing", object{"queue", queue})
					break
				case taskPool <- (<-msg):
					s.debug("got new task from", object{"queue", queue})
				}
			}

		}()
	}

	wg := sync.WaitGroup{}

	for {
		var msg amqp.Delivery

		done := false

		select {
		case msg = <-taskPool:
			s.debug("received a task to process", object{"msgID", msg.MessageId})
		case <-ctx.Done():
			s.debug("task processing stopped")
			done = true
		case err := <-errCh:
			s.error("got error in task: ", err.err, object{"ID", err.taskID})
			s.taskRep.UpdateTaskStatus(err.taskID, err.status, err.err)
			continue
		}

		if done {
			break
		}

		// TODO: retry task if fail. value will be send by header

		hdr := msg.Headers
		if hdr == nil {
			s.warn("skipping headless task")
			continue
		}

		taskID, ok := hdr["TaskID"].(string)
		if !ok {
			s.warn("skipping unidentified task")
			continue
		}

		s.taskRep.UpdateTaskStatus(taskID, util.StatusInProgress, nil)

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

			s.info("processing task", object{"taskName", wrkr.taskName}, object{"taskID", taskID})
			if err := wrkr.process(msg); err != nil {
				errCh <- errorTaskBody{
					taskID,
					util.StatusFailed,
					err,
				}
				return
			}

			s.taskRep.UpdateTaskStatus(taskID, util.StatusSuccess, nil)

		}(wrkr, &msg)
	}

	wg.Wait()

	return err
}

func getQueueName(prefix string, id int) string {
	return prefix + "_" + strconv.Itoa(id)
}
