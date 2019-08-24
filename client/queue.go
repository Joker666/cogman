package client

import (
	"strconv"

	"github.com/Tapfury/cogman/util"

	"github.com/streadway/amqp"
)

func (s *Session) formQueueName(taskType util.TaskPriority) (string, error) {
	queueType := util.LowPriorityQueue
	if taskType == util.TaskPriorityHigh {
		queueType = util.HighPriorityQueue
	}

	id, err := s.getQueueIndex(taskType)
	if err != nil {
		return "", err
	}

	return queueType + "_" + strconv.Itoa(id), nil
}

func (s *Session) getQueueIndex(taskType util.TaskPriority) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := map[string]int{
		util.QueueModeDefault: s.cfg.AMQP.HighPriorityQueueCount,
		util.QueueModeLazy:    s.cfg.AMQP.LowPriorityQueueCount,
	}

	mode := util.QueueModeLazy
	if taskType == util.TaskPriorityHigh {
		mode = util.QueueModeDefault
	}

	if count[mode] == 0 {
		return 0, ErrQueueNotAvailable
	}

	index := s.queueIndex[mode]
	s.queueIndex[mode] = (s.queueIndex[mode] + 1) % count[mode]

	return index, nil
}

// GetQueueName return a available queue name based on task priority
func (s *Session) GetQueueName(pType util.TaskPriority) (string, error) {
	name := ""
	for {
		queue, err := s.formQueueName(pType)
		if err != nil {
			return "", err
		}
		if _, err := s.EnsureQueue(s.conn, queue, pType); err == nil {
			name = queue
			break
		}
	}

	return name, nil
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
