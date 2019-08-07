package cogman

import (
	"log"
	"time"

	"github.com/Tapfury/cogman/client"
	"github.com/Tapfury/cogman/config"
	"github.com/Tapfury/cogman/util"
)

var (
	srvr    *Server
	clnt    *client.Session
	errChan chan error
)

func StartBackground(cfg *config.Config) error {
	serverCfg, clientCfg, err := setConfig(cfg)
	if err != nil {
		return err
	}

	clnt, err = client.NewSession(*clientCfg)
	if err != nil {
		return err
	}

	if err := clnt.Connect(); err != nil {
		return err
	}

	srvr, err = NewServer(*serverCfg)
	if err != nil {
		return err
	}

	go func() {
		defer srvr.Stop()
		if err = srvr.Start(); err != nil {
			log.Print(err)
		}
	}()

	return nil
}

func SendTask(task util.Task, hdlr util.Handler) error {
	if hdlr != nil {
		if err := Register(task.Name, hdlr); err != nil {
			return err
		}
	}

	return clnt.SendTask(task)
}

func Register(taskName string, hdlr util.Handler) error {
	if hdlr == nil || taskName == "" {
		return ErrInvalidData
	}

	return srvr.Register(taskName, hdlr)
}

func setConfig(cfg *config.Config) (*config.Server, *config.Client, error) {
	if cfg == nil {
		cfg = &config.Config{}
	}

	srvrCfg := &config.Server{}
	clntCfg := &config.Client{}

	if cfg.ConnectionTimeout == 0 {
		cfg.ConnectionTimeout = time.Minute * 10
	}

	srvrCfg.ConnectionTimeout = cfg.ConnectionTimeout
	clntCfg.ConnectionTimeout = cfg.ConnectionTimeout

	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = time.Second * 5
	}

	clntCfg.RequestTimeout = cfg.RequestTimeout

	if cfg.AmqpURI == "" {
		return nil, nil, ErrInvalidConfig
	}

	amqp := config.AMQP{
		URI:                    cfg.AmqpURI,
		HighPriorityQueueCount: max(1, cfg.HighPriorityQueueCount),
		LowPriorityQueueCount:  max(1, cfg.LowPriorityQueueCount),
		Exchange:               "",
	}

	srvrCfg.AMQP = amqp
	clntCfg.AMQP = amqp

	if cfg.RedisURI == "" {
		return nil, nil, ErrInvalidConfig
	}

	srvrCfg.Redis = config.Redis{cfg.RedisURI}
	clntCfg.Redis = config.Redis{cfg.RedisURI}

	if cfg.MongoURI == "" {
		cfg.MongoURI = "mongodb://root:secret@localhost:27017"
	}

	srvrCfg.Mongo = config.Mongo{cfg.MongoURI}
	clntCfg.Mongo = config.Mongo{cfg.MongoURI}

	clntCfg.ReEnqueue = cfg.ReEnqueue

	return srvrCfg, clntCfg, nil
}

func max(x int, y int) int {
	if x > y {
		return x
	}
	return y
}
