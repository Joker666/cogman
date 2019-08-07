package main

import (
	"github.com/Tapfury/cogman/config"
)

func getConfig() *config.Config {
	return &config.Config{
		AmqpURI:  "amqp://localhost:5672",
		RedisURI: "redis://localhost:6379/0",

		ReEnqueue: true,
	}
}
