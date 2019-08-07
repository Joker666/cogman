package main

import (
	"log"
	"time"

	"github.com/Tapfury/cogman"
	exampletasks "github.com/Tapfury/cogman/example/tasks"
)

func main() {
	cfg := getConfig()

	log.Print("initiate client & server together")
	cogman.StartBackground(cfg)

	task, err := getMultiplicationTask(21, 7)
	if err != nil {
		log.Fatal(err)
	}

	handler := exampletasks.NewMulTask()
	cogman.SendTask(*task, handler)

	close()
}

func close() {
	log.Print("closing channel...")

	close := time.After(time.Second * 5)
	<-close

	log.Print("channel closed...")
}
