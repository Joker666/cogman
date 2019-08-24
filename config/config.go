package config

import "time"

// Config : Cogman API congig
type Config struct {
	ConnectionTimeout time.Duration
	RequestTimeout    time.Duration

	AmqpURI  string
	RedisURI string
	MongoURI string

	RedisTTL time.Duration
	MongoTTL time.Duration

	HighPriorityQueueCount int
	LowPriorityQueueCount  int
}

// Server : Cogman server config
type Server struct {
	ConnectionTimeout time.Duration

	Mongo Mongo
	Redis Redis
	AMQP  AMQP
}

// Client : Cogman client config
type Client struct {
	ConnectionTimeout time.Duration
	RequestTimeout    time.Duration

	AMQP  AMQP
	Redis Redis
	Mongo Mongo

	ReEnqueue bool
}

// AMQP : rabbitMQ connection config
type AMQP struct {
	URI                    string
	Exchange               string
	HighPriorityQueueCount int
	LowPriorityQueueCount  int
	Prefetch               int
}

// Mongo : Mongo connection config
type Mongo struct {
	URI string
	TTL time.Duration
}

// Redis : Redis connection config
type Redis struct {
	URI string
	TTL time.Duration
}
