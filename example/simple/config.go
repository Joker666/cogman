package main

import (
	"github.com/Tapfury/cogman/config"
)

func getConfig() *config.Config {
	return &config.Config{
		AmqpURI:  "amqp://localhost:5672",    // required
		RedisURI: "redis://localhost:6379/0", // required

		ReEnqueue: true, // optional. default false. Mongo connection also needed
	}
}
