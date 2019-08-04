package cogman

import (
	"context"
	"sync"
	"time"

	"github.com/Tapfury/cogman/util"

	"github.com/streadway/amqp"
)

func (s *Server) Consume(prefetch int) {
	ctx, stop := context.WithCancel(context.Background())

	if err := s.consume(ctx, prefetch); err != nil {
		s.connError <- err
	}

	stop()
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
	var closeErr error

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
				task, err := s.taskRepo.GetTask(errTask.taskID)
				if err != nil {
					s.lgr.Error("failed to get task", err, util.Object{Key: "TaskID", Val: errTask.taskID})
					return
				}

				if orgTask, err := s.taskRepo.GetTask(task.OriginalTaskID); err != nil {
					s.lgr.Error("failed to get task", err, util.Object{Key: "TaskID", Val: orgTask.TaskID})
					return
				} else if orgTask.Retry != 0 {
					go s.retryConn.RetryTask(*orgTask)
				}
			}()

			s.taskRepo.UpdateTaskStatus(errTask.taskID, errTask.status, errTask.err)
			continue
		}

		if done {
			break
		}

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

		s.taskRepo.UpdateTaskStatus(taskID, util.StatusInProgress)

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

			s.taskRepo.UpdateTaskStatus(taskID, util.StatusSuccess, duration)

		}(wrkr, &msg)
	}

	wg.Wait()

	return closeErr
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
