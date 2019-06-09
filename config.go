package cogman

type Config struct {
	MongoURI string
	RedisURI string
	AMQP     AMQP
}

type AMQP struct {
	URI      string
	Exchange string
	Queue    string
	Prefetch int
}
