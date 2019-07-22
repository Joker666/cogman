package client

import (
	"fmt"
	"time"

	"github.com/Tapfury/cogman/util"

	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

// SendTask sends task t
func (s *Session) SendTask(t *util.Task) error {
	s.mu.RLock()
	if !s.connected {
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

	t.ID = uuid.New().String()

	close := ch.NotifyClose(make(chan *amqp.Error))
	publish := ch.NotifyPublish(make(chan amqp.Confirmation))

	Queue := s.GetQueueName(t.Priority)
	errs := make(chan error)

	s.task.CreateTask(t)

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

		s.task.UpdateTaskStatus(t.ID, util.StatusQueued, nil)
	}()

	done := (<-chan time.Time)(make(chan time.Time))
	if s.cfg.RequestTimeout != 0 {
		done = time.After(s.cfg.RequestTimeout)
	}

	select {
	case err := <-close:
		s.task.UpdateTaskStatus(t.ID, util.StatusFailed, err)
		return err
	case err := <-errs:
		s.task.UpdateTaskStatus(t.ID, util.StatusFailed, err)
		return err
	case p := <-publish:
		if !p.Ack {
			s.task.UpdateTaskStatus(t.ID, util.StatusFailed, ErrNotPublished)
			return ErrNotPublished
		}
	case <-done:
		s.task.UpdateTaskStatus(t.ID, util.StatusFailed, ErrRequestTimeout)
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
		queue := fmt.Sprintf("%s_%d", queueType, s.getQueueIndex())
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
