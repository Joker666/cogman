package client

import (
	"time"

	"github.com/Tapfury/cogman/util"

	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

func (s *Session) ReEnqueueUnhandledTasksBefore(t time.Time) error {
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
		s.lgr.Info("Re-Enqueuing", util.Object{"Total Task", len(tasks)})

		skip += limit

		for _, t := range tasks {
			if err := s.SendTask(*t); err != nil {
				return err
			}
		}
	}

	return nil
}

// SendTask sends task t
func (s *Session) SendTask(t util.Task) error {
	if t.TaskID == "" {
		t.TaskID = uuid.New().String()
		if t.OriginalTaskID == "" {
			t.OriginalTaskID = t.TaskID
		}

		if err := s.taskRepo.CreateTask(&t); err != nil {
			return err
		}
	}

	s.mu.RLock()
	if !s.connected {
		s.lgr.Warn("No connection. Task enqueued.", util.Object{"TaskID", t.TaskID})
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

	close := ch.NotifyClose(make(chan *amqp.Error, 1))
	publish := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	publishErr := make(chan error, 1)

	Queue := s.GetQueueName(t.Priority)

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
				Type:         t.Name,
				DeliveryMode: amqp.Persistent,
				Body:         t.Payload,
			},
		)

		if err != nil {
			publishErr <- err
			return
		}

		s.taskRepo.UpdateTaskStatus(t.TaskID, util.StatusQueued)
	}()

	done := (<-chan time.Time)(make(chan time.Time, 1))
	if s.cfg.RequestTimeout != 0 {
		done = time.After(s.cfg.RequestTimeout)
	}

	var errs error

	select {
	case errs = <-close:

	case errs = <-publishErr:

	case p := <-publish:
		if !p.Ack {
			s.lgr.Warn("Task deliver failed", util.Object{"TaskID", t.TaskID})
			errs = ErrNotPublished
		}
		s.lgr.Info("Task delivered", util.Object{"TaskID", t.TaskID})
	case <-done:
		errs = ErrRequestTimeout
	}

	if errs != nil {
		orgTask, err := s.taskRepo.GetTask(t.OriginalTaskID)
		if err != nil {
			s.lgr.Error("failed to get task", err, util.Object{"TaskID", t.OriginalTaskID})
		} else {
			s.taskRepo.UpdateTaskStatus(t.TaskID, util.StatusFailed, errs)
			if orgTask.Retry != 0 {
				go func() {
					s.retryTask(t)
				}()
			}
		}
	}

	return errs
}

func (s *Session) retryTask(t util.Task) {
	task := util.Task{
		Name:           t.Name,
		OriginalTaskID: t.OriginalTaskID,
		Payload:        t.Payload,
		Priority:       t.Priority,
		Status:         util.StatusRetry,
	}

	if err := s.SendTask(task); err != nil {
		s.lgr.Error("failed to retry", err, util.Object{"TaskID", task.TaskID}, util.Object{"OriginalTaskID", task.OriginalTaskID})
	}
}
