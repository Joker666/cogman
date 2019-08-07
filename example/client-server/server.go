package main

import (
	"log"
	"time"

	"github.com/Tapfury/cogman"
	"github.com/Tapfury/cogman/config"
)

func SetupServer() (*cogman.Server, error) {
	cfg := config.Server{
		ConnectionTimeout: time.Minute * 10,

		Mongo: config.Mongo{URI: "mongodb://root:secret@localhost:27017/", TTL: time.Hour},
		Redis: config.Redis{URI: "redis://localhost:6379/0", TTL: time.Hour},
		AMQP: config.AMQP{
			URI:                    "amqp://localhost:5672",
			HighPriorityQueueCount: 5,
			LowPriorityQueueCount:  5,
			Exchange:               "",
		},
	}

	srvr, err := cogman.NewServer(cfg)
	if err != nil {
		return nil, err
	}

	go func() {
		defer srvr.Stop()
		if err = srvr.Start(); err != nil {
			log.Print(err)
		}
	}()

	return srvr, nil
}
