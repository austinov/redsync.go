package redsync

import (
	"github.com/garyburd/redigo/redis"
	"testing"
	"time"
	"net"
	"math/rand"
	"github.com/austinov/redsync.go/redsync"
)

var addrs = []net.Addr{
	&net.TCPAddr{Port: 63790},
	&net.TCPAddr{Port: 63791},
	&net.TCPAddr{Port: 63792},
	&net.TCPAddr{Port: 63793},
}

func getRedisPool(network, server, password string) (pool *redis.Pool) {
	pool = &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial(network, server)
			if err != nil {
				return nil, err
			}
			if password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, _ time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}

	return pool
}

// Implementation of RedisExecutor interface
type RedigoExecutor struct {
	Conn *redis.Conn
}

// Implementation of RedisExecutor interface
func (this *RedigoExecutor) DoSetNxPx(key, value string, milliseconds int) (bool, error) {
	conn := *this.Conn
	reply, err := redis.String(conn.Do("SET", key, value, "NX", "PX", milliseconds))
	return (reply == "OK"), err
}

// Implementation of RedisExecutor interface
func (this *RedigoExecutor) DoScript(script string, key string, arg string) error {
	conn := *this.Conn
	s := redis.NewScript(1, script)
	_, err := s.Do(conn, key, arg)
	return err
}

func TestRedlock(t *testing.T) {

	executors := make([]redsync.RedisExecutor, 0)
	for _, addr := range addrs {
		pool := getRedisPool(addr.Network(), addr.String(), "")
		conn := pool.Get()
		executors = append(executors, &RedigoExecutor{ &conn })
	}
	defer func() {
		for _, executor := range executors {
			if executor != nil {
				redigoConn := executor.(*RedigoExecutor)
				(*redigoConn.Conn).Close()
			}
		}
	}()

	done := make(chan bool)
	chErr := make(chan error)
	for i := 0; i < 1; i++ {
		go func() {
			m, err := redsync.NewMutex("RedsyncMutex", executors)
			if err != nil {
				chErr <- err
				return
			}

			f := 0
			for j := 0; j < 32; j++ {
				err := m.Lock()
				if err == redsync.ErrFailed {
					f += 1
					if f > 2 {
						chErr <- err
						return
					}
					continue
				}
				if err != nil {
					chErr <- err
					return
				}

				time.Sleep(1 * time.Millisecond)

				m.Unlock()

				time.Sleep(time.Duration(rand.Int31n(128)) * time.Millisecond)
			}
			done <- true
		}()
	}

	for i := 0; i < 1; i++ {
		select {
		case <-done:
		case err := <-chErr:
			t.Fatal(err)
		}
	}
}
