package main

import (
	"log"

	"github.com/Tapfury/cogman"
	"github.com/Tapfury/cogman/config"
	exampletasks "github.com/Tapfury/cogman/example/tasks"
)

func main() {

	task := []config.Task{
		config.Task{
			Name:  exampletasks.TaskAddition,
			Retry: 0,
		},
		config.Task{
			Name:  exampletasks.TaskMultiplication,
			Retry: 0,
		},
		config.Task{
			Name:  exampletasks.TaskSubtraction,
			Retry: 0,
		},
	}

	cfg := config.Server{
		Mongo: config.Mongo{URI: "mongodb://root:secret@localhost:27017/"},
		Redis: config.Redis{URI: "redis://localhost:6379/0"},
		AMQP: config.AMQP{
			URI:                    "amqp://localhost:5672",
			HighPriorityQueueCount: 5,
			LowPriorityQueueCount:  5,
			Exchange:               "",
		},
		Tasks: task,
	}

	log.Print("creating new server")
	srv, err := cogman.NewServer(cfg)
	if err != nil {
		log.Print(err)
		return
	}

	workerSum := exampletasks.NewSumTask()
	if err = srv.Register(workerSum.Name, workerSum); err != nil {
		log.Print("failed to register sum worker", err)
		return
	}

	workerSub := exampletasks.NewSubTask()
	if err = srv.Register(workerSub.Name, workerSub); err != nil {
		log.Print("failed to register sub worker", err)
		return
	}

	workerMul := exampletasks.NewMulTask()
	if err = srv.Register(workerMul.Name, workerMul); err != nil {
		log.Print("failed to register mul worker", err)
		return
	}

	log.Print("waiting for task...")
	srv.Start()

	defer srv.Stop()
}
