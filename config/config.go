package config

import "time"

type Server struct {
	Mongo Mongo
	Redis Redis
	AMQP  AMQP
	Tasks []Task
}

type AMQP struct {
	URI      string
	Exchange string
	Queue    string
	Prefetch int
}

type Mongo struct {
	URI string
}

type Redis struct {
	URI string
}

type Task struct {
	Name  string
	Retry int
}

type Client struct {
	ConnectionTimeout time.Duration

	RequestTimeout time.Duration
	AMQP           AMQP
}
