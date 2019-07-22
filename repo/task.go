package repo

import (
	"log"
	"time"

	"github.com/Tapfury/cogman/infra"
	"github.com/Tapfury/cogman/util"
)

type Task struct {
	RedisConn *infra.RedisClient
	MongoConn *infra.MongoClient
}

func (s *Task) CloseClients() {
	s.RedisConn.Close()
	s.MongoConn.Close()
}

func (s *Task) CreateTask(task *util.Task) {
	go func() {
		err := s.MongoConn.CreateTask(task)
		if err != nil {
			log.Print("Mongo: failed to create task ", err)
		}
	}()

	err := s.RedisConn.CreateTask(task)
	if err != nil {
		log.Print("Redis: failed to create task ", err)
	}
}

func (s *Task) UpdateTaskStatus(id string, status util.Status, failError error) {
	go func() {
		var err error
		for i := 0; i < 3; i++ {
			if err = s.MongoConn.UpdateTaskStatus(id, status, failError); err == nil {
				return
			}
			time.Sleep(time.Second)
		}
		log.Print("Mongo: failed to update task: ", id, " ", status, " ", err)
	}()

	err := s.RedisConn.UpdateTaskStatus(id, status, failError)
	if err != nil {
		log.Print("Redis: failed to update task: ", id, " ", status, " ", err)
	}
}
