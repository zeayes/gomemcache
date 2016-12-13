package gomemcache

import (
	"errors"
	"net"
	"sync"
	"time"
)

var nowFunc = time.Now // for testing

var (
	// ErrPoolExhausted idle connection pool exhausted
	ErrPoolExhausted = errors.New("connection pool exhausted")
	errPoolClosed    = errors.New("pool is closed ")
)

// Conn connection used in pool
type Conn net.Conn

// Pool goroutine safe conn pool
type Pool struct {
	DialFunc      func() (Conn, error)
	MaxIdleConns  int
	IdleTimeout   time.Duration
	SocketTimeout time.Duration

	closed    bool
	mu        sync.Mutex
	idleConns []*idleConn
}

// Conn net connection with idle timeout
type idleConn struct {
	conn   Conn
	idleAt time.Time
}

// Get get a connection from idle conns
func (pool *Pool) Get() (Conn, error) {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	idleConns := pool.idleConns[:0]
	expiredSince := nowFunc().Add(-pool.IdleTimeout)
	for _, ic := range pool.idleConns {
		if ic.idleAt.Before(expiredSince) {
			if err := ic.conn.Close(); err != nil {
				return nil, err
			}
		} else {
			idleConns = append(idleConns, ic)
		}
	}
	numIdle := len(idleConns)
	if numIdle >= pool.MaxIdleConns {
		return nil, ErrPoolExhausted
	}
	if numIdle == 0 {
		pool.idleConns = idleConns
		if pool.closed {
			return nil, errPoolClosed
		}
		c, err := pool.DialFunc()
		if err != nil {
			return nil, err
		}
		return c, nil
	}
	pool.idleConns = idleConns[:numIdle-1]
	return idleConns[numIdle-1].conn, nil
}

// Put put an idle conn into idle conns
func (pool *Pool) Put(c Conn) error {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.closed || len(pool.idleConns) >= pool.MaxIdleConns {
		return c.Close()
	}
	ic := idleConn{conn: c, idleAt: nowFunc()}
	pool.idleConns = append(pool.idleConns, &ic)
	return nil
}

// Close close all connections in pool
func (pool *Pool) Close() error {
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return nil
	}
	for _, ic := range pool.idleConns {
		if err := ic.conn.Close(); err != nil {
			pool.mu.Unlock()
			return err
		}
	}
	pool.closed = true
	pool.idleConns = nil
	pool.mu.Unlock()
	return nil
}
