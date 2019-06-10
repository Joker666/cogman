package config

type Server struct {
	Mongo Mongo
	Redis Redis
	AMQP  AMQP
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
