package main

import (
	"time"

	"github.com/Tapfury/cogman/client"
	"github.com/Tapfury/cogman/config"
)

func SetupClient() (*client.Session, error) {
	cfg := config.Client{
		ConnectionTimeout: time.Minute * 10,
		RequestTimeout:    time.Second * 10,

		Mongo: config.Mongo{URI: "mongodb://root:secret@localhost:27017/", TTL: time.Hour},
		Redis: config.Redis{URI: "redis://localhost:6379/0", TTL: time.Hour},
		AMQP: config.AMQP{
			URI:                    "amqp://localhost:5672",
			HighPriorityQueueCount: 5,
			LowPriorityQueueCount:  5,
			Exchange:               "",
		},

		ReEnqueue: true,
	}

	clnt, err := client.NewSession(cfg)
	if err != nil {
		return nil, err
	}

	if err := clnt.Connect(); err != nil {
		return nil, err
	}

	return clnt, nil
}
