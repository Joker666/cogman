package repo

import (
	"log"

	"github.com/Tapfury/cogman/infra"
	"github.com/Tapfury/cogman/util"
)

type Task struct {
	RedisConn *infra.RedisClient
	MongoConn *infra.MongoClient
}

func (s *Task) Close() {
	s.RedisConn.Close()
	s.MongoConn.Close()
}

func (s *Task) CreateTask(task *util.Task) {
	go func() {
		err := s.MongoConn.CreateTask(task)
		if err != nil {
			log.Print("Mongo: faile to create task ", err)
		}
	}()

	err := s.RedisConn.CreateTask(task)
	if err != nil {
		log.Print("Redis: faile to create task ", err)
	}
}

func (s *Task) UpdateTaskStatus(id string, status util.Status, failError error) {
	go func() {
		err := s.MongoConn.UpdateTaskStatus(id, status, failError)
		if err != nil {
			log.Print("Mongo: faile to update task: ", id, " ", err)
		}
	}()

	err := s.RedisConn.UpdateTaskStatus(id, status, failError)
	if err != nil {
		log.Print("Redis: faile to update task: ", id, " ", err)
	}
}
