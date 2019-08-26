package main

import (
	"log"
	"time"

	"github.com/Tapfury/cogman"
	"github.com/Tapfury/cogman/config"
	exampletasks "github.com/Tapfury/cogman/example/tasks"
)

func main() {
	cfg := config.Server{
		ConnectionTimeout: time.Minute * 10, // default .server will stop trying to reconnection if it is not done within connection timeout

		Mongo: config.Mongo{URI: "mongodb://root:secret@localhost:27017/", TTL: time.Hour}, // default
		Redis: config.Redis{URI: "redis://localhost:6379/0", TTL: time.Hour},               // required
		AMQP: config.AMQP{
			URI:                    "amqp://localhost:5672",
			HighPriorityQueueCount: 2,  // default
			LowPriorityQueueCount:  5,  // default
			Exchange:               "", // Client and Server exchange should be same
		},
	}

	srvr, err := cogman.NewServer(cfg)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		defer srvr.Stop()
		if err = srvr.Start(); err != nil {
			log.Fatal(err)
		}
	}()

	// Task handler register
	srvr.Register(exampletasks.TaskAddition, exampletasks.NewSumTask())
	srvr.Register(exampletasks.TaskSubtraction, exampletasks.NewSubTask())
	srvr.Register(exampletasks.TaskMultiplication, exampletasks.NewMulTask())

	log.Print("[x] press ctrl + c to terminate the program")
	close := make(chan struct{})
	<-close
}
