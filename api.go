package cogman

import (
	"log"
	"time"

	"github.com/Joker666/cogman/client"
	"github.com/Joker666/cogman/config"
	"github.com/Joker666/cogman/util"
)

var (
	server        *Server
	clientSession *client.Session
)

// StartBackground starts the server and client in background
func StartBackground(cfg *config.Config) error {
	serverCfg, clientCfg, err := setConfig(cfg)
	if err != nil {
		return err
	}

	clientSession, err = client.NewSession(*clientCfg)
	if err != nil {
		return err
	}

	if err := clientSession.Connect(); err != nil {
		return err
	}

	server, err = NewServer(*serverCfg)
	if err != nil {
		return err
	}

	go func() {
		defer server.Stop()
		if err = server.Start(); err != nil {
			log.Print(err)
		}
	}()

	return nil
}

// SendTask sends task to server from client
func SendTask(task util.Task, handler util.Handler) error {
	if handler != nil {
		if err := Register(task.Name, handler); err != nil {
			return err
		}
	}

	return clientSession.SendTask(task)
}

// Register registers task for server to process
func Register(taskName string, handler util.Handler) error {
	if handler == nil || taskName == "" {
		return ErrInvalidData
	}

	return server.Register(taskName, handler)
}

func setConfig(cfg *config.Config) (*config.Server, *config.Client, error) {
	if cfg == nil {
		cfg = &config.Config{}
	}

	serverCfg := &config.Server{}
	clientCfg := &config.Client{}

	if cfg.ConnectionTimeout == 0 {
		cfg.ConnectionTimeout = time.Minute * 10
	}

	serverCfg.ConnectionTimeout = cfg.ConnectionTimeout
	clientCfg.ConnectionTimeout = cfg.ConnectionTimeout

	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = time.Second * 5
	}

	clientCfg.RequestTimeout = cfg.RequestTimeout

	if cfg.AmqpURI == "" {
		return nil, nil, ErrInvalidConfig
	}

	amqp := config.AMQP{
		URI:                    cfg.AmqpURI,
		HighPriorityQueueCount: max(1, cfg.HighPriorityQueueCount),
		LowPriorityQueueCount:  max(1, cfg.LowPriorityQueueCount),
		Exchange:               "",
		Prefetch:               cfg.Prefetch,
	}

	serverCfg.AMQP = amqp
	clientCfg.AMQP = amqp

	if cfg.RedisURI == "" {
		return nil, nil, ErrInvalidConfig
	}

	if cfg.RedisTTL == 0 {
		cfg.RedisTTL = time.Hour * 24 * 7 // 1 week
	}

	serverCfg.Redis = config.Redis{URI: cfg.RedisURI, TTL: cfg.RedisTTL}
	clientCfg.Redis = config.Redis{URI: cfg.RedisURI, TTL: cfg.RedisTTL}

	if cfg.MongoTTL == 0 {
		cfg.MongoTTL = time.Hour * 24 * 30 // 1  month
	}

	serverCfg.Mongo = config.Mongo{URI: cfg.MongoURI, TTL: cfg.MongoTTL}
	clientCfg.Mongo = config.Mongo{URI: cfg.MongoURI, TTL: cfg.MongoTTL}

	serverCfg.StartRestServer = cfg.StartRestServer
	return serverCfg, clientCfg, nil
}

func max(x int, y int) int {
	if x > y {
		return x
	}
	return y
}
