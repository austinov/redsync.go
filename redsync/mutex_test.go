package redsync_test

import (
	"github.com/fzzy/radix/redis"
	"math/rand"
	"net"
	"testing"
	"time"
	"github.com/austinov/redsync.go/redsync"
)

var addrs = []net.Addr{
	&net.TCPAddr{Port: 63790},
	&net.TCPAddr{Port: 63791},
	&net.TCPAddr{Port: 63792},
	&net.TCPAddr{Port: 63793},
}

// Implementation of RedisExecutor interface
type RadixExecutor struct {
	Conn *redis.Client
}

// Implementation of RedisExecutor interface
func (this *RadixExecutor) DoSetNxPx(key, value string, milliseconds int) (bool, error) {
	reply := this.Conn.Cmd("set", key, value, "nx", "px", milliseconds)
	return (reply.String() == "OK"), reply.Err
}

// Implementation of RedisExecutor interface
func (this *RadixExecutor) DoScript(script string, key string, arg string) error {
	reply := this.Conn.Cmd("eval", script, 1, key, arg)
	return reply.Err
}

func TestMutex(t *testing.T) {
	done := make(chan bool)
	chErr := make(chan error)

	executors := make([]redsync.RedisExecutor, 0)
	for _, addr := range addrs {
		if conn, err := redis.Dial(addr.Network(), addr.String()); err != nil {
			t.Fatal(err)
		} else {
			executors = append(executors, &RadixExecutor{ conn })
		}
	}
	defer func() {
		for _, executor := range executors {
			if executor != nil {
				radixConn := executor.(*RadixExecutor)
				(*radixConn.Conn).Close()
			}
		}
	}()

	for i := 0; i < len(addrs); i++ {
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
	for i := 0; i < len(addrs); i++ {
		select {
		case <-done:
		case err := <-chErr:
			t.Fatal(err)
		}
	}
}
