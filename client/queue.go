package client

import (
	"strconv"

	"github.com/Tapfury/cogman/util"

	"github.com/streadway/amqp"
)

func (s *Session) getQueueName(taskType util.TaskPriority) string {
	queueType := util.LowPriorityQueue
	if taskType == util.TaskPriorityHigh {
		queueType = util.HighPriorityQueue
	}

	id := s.getQueueIndex(taskType)

	return queueType + "_" + strconv.Itoa(id)
}

func (s *Session) getQueueIndex(taskType util.TaskPriority) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	index := 0
	switch taskType {
	case util.TaskPriorityHigh:
		index = s.queueIndex[util.QueueModeDefault]
		s.queueIndex[util.QueueModeDefault] = (s.queueIndex[util.QueueModeDefault] + 1) % s.cfg.AMQP.HighPriorityQueueCount
	case util.TaskPriorityLow:
		index = s.queueIndex[util.QueueModeLazy]
		s.queueIndex[util.QueueModeLazy] = (s.queueIndex[util.QueueModeLazy] + 1) % s.cfg.AMQP.LowPriorityQueueCount
	}

	return index
}

func (s *Session) GetQueueName(pType util.TaskPriority) string {
	name := ""
	for {
		queue := s.getQueueName(pType)
		if _, err := s.EnsureQueue(s.conn, queue, pType); err == nil {
			name = queue
			break
		}
	}

	return name
}

func (s *Session) EnsureQueue(con *amqp.Connection, queue string, taskType util.TaskPriority) (*amqp.Queue, error) {
	chnl, err := con.Channel()
	if err != nil {
		return nil, err
	}
	defer chnl.Close()

	mode := util.QueueModeLazy
	if taskType == util.TaskPriorityHigh {
		mode = util.QueueModeDefault
	}

	qu, err := chnl.QueueDeclare(
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
		return nil, err
	}

	return &qu, nil
}
