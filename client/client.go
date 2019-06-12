package client

import (
	"github.com/Tapfury/cogman/config"
)

type Client struct {
	cfg *config.Client
}

type Task struct {
	Name string
	Payload []byte
	
	id string
}

func (t *Task)ID() string {
	return t.id
}

func (c *Client)SendTask(t *Task) error {
	
}