package client

import (
	"context"
	"time"

	"github.com/Tapfury/cogman/util"

	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

func (s *Session) reEnqueueUnhandledTasksBefore(t time.Time) error {
	limit := 20
	skip := 0

	for {
		tasks, err := s.taskRepo.ListByStatusBefore(util.StatusInitiated, t, skip, limit)
		if err != nil {
			return err
		}

		if len(tasks) == 0 {
			break
		}
		s.lgr.Info("Re-Enqueuing", util.Object{Key: "Total Task", Val: len(tasks)})

		skip += limit

		for _, t := range tasks {
			if err := s.SendTask(*t); err != nil {
				return err
			}
		}
	}

	return nil
}

// SendTask sends a task to rabbitmq
func (s *Session) SendTask(t util.Task) error {
	// Checking taskID. re-enqueued task will be skipped
	if t.TaskID == "" {
		t.TaskID = uuid.New().String()
		if t.OriginalTaskID == "" {
			t.OriginalTaskID = t.TaskID
		}

		if err := s.taskRepo.CreateTask(&t); err != nil {
			return err
		}
	}

	// Checking AMQP connection. Task will be logged for no connection. Re-enqueued later.
	s.mu.RLock()
	if !s.connected {
		s.lgr.Warn("No connection. Task enqueued.", util.Object{Key: "TaskID", Val: t.TaskID})
		return ErrNotConnected
	}
	s.mu.RUnlock()

	if !t.Priority.Valid() {
		return ErrInvalidPriority
	}

	ch, err := s.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	if err := ch.Confirm(false); err != nil {
		return err
	}

	closeNotification := ch.NotifyClose(make(chan *amqp.Error, 1))
	publish := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	publishErr := make(chan error, 1)

	Queue, err := s.GetQueueName(t.Priority)
	if err != nil {
		return err
	}

	go func() {
		err := ch.Publish(
			s.cfg.AMQP.Exchange,
			Queue,
			false,
			false,
			amqp.Publishing{
				Headers: map[string]interface{}{
					"TaskName": t.Name,
					"TaskID":   t.TaskID,
				},
				MessageId:    t.TaskID,
				DeliveryMode: amqp.Persistent,
				Body:         t.Payload,
			},
		)

		if err != nil {
			publishErr <- err
			return
		}

		s.taskRepo.UpdateTaskStatus(context.Background(), t.TaskID, util.StatusQueued)
	}()

	done := (<-chan time.Time)(make(chan time.Time, 1))
	if s.cfg.RequestTimeout != 0 {
		done = time.After(s.cfg.RequestTimeout)
	}

	var errs error

	select {
	case errs = <-closeNotification:

	case errs = <-publishErr:

	case p := <-publish:
		if !p.Ack {
			s.lgr.Warn("Task deliver failed", util.Object{Key: "TaskID", Val: t.TaskID})
			errs = ErrNotPublished
			break
		}
		s.lgr.Info("Task delivered", util.Object{Key: "TaskID", Val: t.TaskID})
	case <-done:
		errs = ErrRequestTimeout
	}

	// For any kind of error, task will be retried if retry count non zero.
	// TODO: retry count only reduce for task processing related error.
	if errs != nil {
		if orgTask, err := s.taskRepo.GetTask(t.OriginalTaskID); err != nil {
			s.lgr.Error("failed to get task", err, util.Object{Key: "TaskID", Val: t.OriginalTaskID})
		} else if orgTask.Retry != 0 {
			go s.RetryTask(t)
		}

		s.taskRepo.UpdateTaskStatus(context.Background(), t.TaskID, util.StatusFailed, errs)
	}

	return errs
}

// RetryTask re-process the task reducing counter by 1
func (s *Session) RetryTask(t util.Task) error {
	task := util.Task{
		Name:           t.Name,
		OriginalTaskID: t.OriginalTaskID,
		Payload:        t.Payload,
		Priority:       t.Priority,
		Status:         util.StatusRetry,
	}

	// updating original task id counter
	s.taskRepo.UpdateRetryCount(t.OriginalTaskID, -1)
	if err := s.SendTask(task); err != nil {
		s.lgr.Error("failed to retry", err, util.Object{Key: "TaskID", Val: task.TaskID}, util.Object{"OriginalTaskID", task.OriginalTaskID})
		return err
	}

	return nil
}
