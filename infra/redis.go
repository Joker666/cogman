package infra

import (
	"time"

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

func (s *RedisClient) Get(key string) ([]byte, error) {
	conn := s.rcon.Get()
	if conn == nil {
		return nil, ErrNotConnected
	}
	defer conn.Close()

	return redis.Bytes(conn.Do("GET", key))
}

func (s *RedisClient) Create(key string, t []byte) error {
	conn := s.rcon.Get()
	if conn == nil {
		return ErrNotConnected
	}
	defer conn.Close()

	_, err := conn.Do("SET", key, t)
	return err
}

func (s *RedisClient) Update(key string, t []byte) error {
	conn := s.rcon.Get()
	if conn == nil {
		return ErrNotConnected
	}
	defer conn.Close()

	_, err := conn.Do("SET", key, t)
	return err
}
