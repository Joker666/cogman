package main

import (
	"time"

	"github.com/Tapfury/cogman/config"
)

func getConfig() *config.Config {
	return &config.Config{
		ConnectionTimeout: time.Minute * 10, // optional
		RequestTimeout:    time.Second * 5,  // optional

		AmqpURI:  "amqp://localhost:5672",                  // required
		RedisURI: "redis://localhost:6379/0",               // required
		MongoURI: "mongodb://root:secret@localhost:27017/", //optional

		HighPriorityQueueCount: 2, // Optional. Default value 1
		LowPriorityQueueCount:  1, // Optional. Default value 1

		ReEnqueue: true, // optional. default false. Mongo connection also needed
	}
}
