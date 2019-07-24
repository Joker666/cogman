package client

import (
	"strconv"
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
	if t.ID == "" {
		t.ID = uuid.New().String()
		if err := s.taskRepo.CreateTask(&t); err != nil {
			return err
		}
	}

	s.mu.RLock()
	if !s.connected {
		s.lgr.Warn("No connection. Task enqueued.", util.Object{"TaskID", t.ID})
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
	errs := make(chan error, 1)

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
					"TaskID":   t.ID,
				},
				Type:         t.Name,
				DeliveryMode: amqp.Persistent,
				Body:         t.Payload,
			},
		)

		if err != nil {
			errs <- err
			return
		}

		s.taskRepo.UpdateTaskStatus(t.ID, util.StatusQueued)
	}()

	done := (<-chan time.Time)(make(chan time.Time, 1))
	if s.cfg.RequestTimeout != 0 {
		done = time.After(s.cfg.RequestTimeout)
	}

	select {
	case err := <-close:
		s.taskRepo.UpdateTaskStatus(t.ID, util.StatusFailed, err)
		return err
	case err := <-errs:
		s.taskRepo.UpdateTaskStatus(t.ID, util.StatusFailed, err)
		return err
	case p := <-publish:
		if !p.Ack {
			s.lgr.Warn("Task acknowledgement failed", util.Object{"TaskID", t.ID})
			s.taskRepo.UpdateTaskStatus(t.ID, util.StatusFailed, ErrNotPublished)
			return ErrNotPublished
		}
		s.lgr.Info("Task acknowledged", util.Object{"TaskID", t.ID})
	case <-done:
		s.taskRepo.UpdateTaskStatus(t.ID, util.StatusFailed, ErrRequestTimeout)
		return ErrRequestTimeout
	}

	return nil
}

func (s *Session) GetQueueName(pType util.PriorityType) string {
	queueType := util.LowPriorityQueue
	name := ""

	if pType == util.PriorityTypeHigh {
		queueType = util.HighPriorityQueue
	}

	for {
		// TODO: Handle low priority queue
		queue := getQueueName(queueType, s.getQueueIndex())
		if _, err := s.EnsureQueue(s.conn, queue); err == nil {
			name = queue
			break
		}
	}

	return name
}

func (s *Session) getQueueIndex() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	index := s.queueIndex
	s.queueIndex++
	s.queueIndex = s.queueIndex % s.cfg.AMQP.HighPriorityQueueCount

	return index
}

func (s *Session) EnsureQueue(con *amqp.Connection, queue string) (*amqp.Queue, error) {
	chnl, err := con.Channel()
	if err != nil {
		return nil, err
	}

	defer chnl.Close()

	qu, err := chnl.QueueDeclare(
		queue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return &qu, nil
}

func getQueueName(prefix string, id int) string {
	return prefix + "_" + strconv.Itoa(id)
}
