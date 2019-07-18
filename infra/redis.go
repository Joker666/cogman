package infra

import (
	"encoding/json"
	"time"

	"github.com/Tapfury/cogman/util"
	"github.com/gomodule/redigo/redis"
)

type RedisClient struct {
	URL  string
	rcon *redis.Pool
}

func NewRedisClient(url string) *RedisClient {
	return &RedisClient{
		URL: url,
		rcon: &redis.Pool{
			MaxIdle:     5,
			IdleTimeout: 300 * time.Second,
			Dial: func() (redis.Conn, error) {
				return redis.DialURL(url)
			},
		},
	}
}

func (s *RedisClient) Ping() error {
	conn := s.rcon.Get()
	if conn == nil {
		return ErrNotConnected
	}
	defer conn.Close()

	if _, err := conn.Do("PING"); err != nil {
		return err
	}

	return nil
}

func (s *RedisClient) Close() error {
	return s.rcon.Close()
}

func (s *RedisClient) getTask(id string) (*util.Task, error) {
	conn := s.rcon.Get()
	if conn == nil {
		return nil, ErrNotConnected
	}
	defer conn.Close()

	byts, err := redis.Bytes(conn.Do("GET", id))
	if err != nil {
		return nil, err
	}

	t := util.Task{}
	if err := json.Unmarshal(byts, &t); err != nil {
		return nil, err
	}

	return &t, nil
}

func (s *RedisClient) CreateTask(t *util.Task) error {
	conn := s.rcon.Get()
	if conn == nil {
		return ErrNotConnected
	}
	defer conn.Close()

	nw := time.Now()

	t.Status = util.StatusInitiated
	t.UpdatedAt = nw
	t.CreatedAt = nw

	byts, err := json.Marshal(t)
	if err != nil {
		return err
	}

	_, err = conn.Do("SET", t.ID, byts)
	return err
}

func (s *RedisClient) UpdateTaskStatus(id string, status util.Status, failErr error) error {
	conn := s.rcon.Get()
	if conn == nil {
		return ErrNotConnected
	}
	defer conn.Close()

	t, err := s.getTask(id)
	if err != nil {
		return err
	}

	if t.Status == util.StatusSuccess {
		return nil
	}

	// A failed task won't be overwrite by any new error message
	if status == util.StatusFailed &&
		t.FailError != "" {
		return nil
	}

	t.Status = status
	t.UpdatedAt = time.Now()
	t.FailError = failErr.Error()

	byts, err := json.Marshal(t)
	if err != nil {
		return err
	}

	_, err = conn.Do("SET", t.ID, byts)
	return err
}
