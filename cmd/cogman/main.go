package main

import (
	"fmt"
	"log"

	"github.com/Joker666/cogman"
	"github.com/Joker666/cogman/config"
	"github.com/Joker666/cogman/version"
)

func main() {
	fmt.Println("Version", version.Version)
	cfg := config.Server{
		Mongo: config.Mongo{URI: "mongodb://root:secret@localhost:27017/"},
		Redis: config.Redis{URI: "redis://localhost:6379/0"},
		AMQP:  config.AMQP{URI: "amqp://localhost:5672"},
	}
	_, err := cogman.NewServer(cfg)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Hello")
}
