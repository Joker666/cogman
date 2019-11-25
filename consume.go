package cogman

import (
	"context"
	"sync"
	"time"

	"github.com/Tapfury/cogman/util"

	"github.com/streadway/amqp"
)

// Consume start task consumption
func (s *Server) Consume(ctx context.Context, prefetch int) {
	ctx, stop := context.WithCancel(ctx)

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

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (s *Server) consume(ctx context.Context, prefetch int) error {
	errCh := make(chan errorTaskBody, 1)

	s.lgr.Debug("creating channel")

	taskPool := make(chan amqp.Delivery)

	s.lgr.Debug("creating consumer")
	for i := 0; i < s.cfg.AMQP.HighPriorityQueueCount; i++ {
		queue := formQueueName(util.HighPriorityQueue, i)
		go s.setConsumer(ctx, queue, util.QueueModeDefault, maxInt(20, prefetch), taskPool)
	}

	for i := 0; i < s.cfg.AMQP.LowPriorityQueueCount; i++ {
		queue := formQueueName(util.LowPriorityQueue, i)
		go s.setConsumer(ctx, queue, util.QueueModeLazy, maxInt(10, prefetch), taskPool)
	}

	wg := sync.WaitGroup{}
	var closeErr error

	// waiting for task response from queue consumer.
	for {
		var msg amqp.Delivery

		done := false

		select {
		case <-ctx.Done():
			s.lgr.Debug("task processing stopped")
			done = true
		case msg = <-taskPool:
			s.lgr.Debug("received a task to process", util.Object{Key: "TaskID", Val: msg.MessageId})
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

		if err := msg.Ack(true); err != nil {
			s.lgr.Warn("fail to ack")
			continue
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
		// Start processing task
		go func(wrkr *util.Worker, msg *amqp.Delivery) {
			defer wg.Done()

			s.lgr.Info("processing task", util.Object{Key: "taskName", Val: wrkr.Name()}, util.Object{Key: "taskID", Val: taskID})
			startAt := time.Now()
			if err := wrkr.Process(msg); err != nil {
				errCh <- errorTaskBody{
					taskID,
					util.StatusFailed,
					err,
				}
				return
			}
			duration := float64(time.Since(startAt)) / float64(time.Second)

			s.taskRepo.UpdateTaskStatus(taskID, util.StatusSuccess, duration)

		}(wrkr, &msg)
	}

	wg.Wait()

	return closeErr
}

// setConsumer set a consumer for each single queue.
func (s *Server) setConsumer(ctx context.Context, queue, mode string, prefetch int, taskPool chan<- amqp.Delivery) {
	chnl, err := s.acon.Channel()
	if err != nil {
		s.lgr.Error("failed to create channel", err)
		return
	}

	defer chnl.Close()

	closeNotification := chnl.NotifyClose(make(chan *amqp.Error, 1))

	if err := chnl.Qos(prefetch, 0, false); err != nil {
		s.lgr.Error("failed to set qos", err)
		return
	}

	msg, err := chnl.Consume(
		queue,
		"",
		false,
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
		case closeErr := <-closeNotification:
			s.lgr.Error("queue closing", closeErr, util.Object{Key: "queue", Val: queue})
			return
		case <-ctx.Done():
			s.lgr.Debug("queue closing", util.Object{Key: "queue", Val: queue})
			return
		case taskPool <- <-msg:
			s.lgr.Debug("new task", util.Object{Key: "queue", Val: queue})
		}
	}
}
