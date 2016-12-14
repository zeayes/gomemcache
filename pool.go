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

// Pool goroutine safe connection pool
type Pool struct {
	DialFunc      func() (Conn, error)
	MaxIdleConns  int
	IdleTimeout   time.Duration
	SocketTimeout time.Duration

	closed    bool
	mu        sync.Mutex
	idleConns []*idleConn // idle connections list, latest connection appending the last
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
	if pool.closed {
		return nil, errPoolClosed
	}
	expiredSince := nowFunc().Add(-pool.IdleTimeout)
	index := len(pool.idleConns)
	for idx, ic := range pool.idleConns {
		// find the first active connection, behind connections must be active.
		if ic.idleAt.After(expiredSince) {
			index = idx
			break
		}
		// close expired connection
		if err := ic.conn.Close(); err != nil {
			return nil, err
		}
	}
	pool.idleConns = pool.idleConns[index:]
	numIdle := len(pool.idleConns)
	if numIdle == 0 {
		return pool.DialFunc()
	}
	conn := pool.idleConns[numIdle-1]
	pool.idleConns = pool.idleConns[:numIdle-1]
	return conn.conn, nil
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
	defer pool.mu.Unlock()
	if pool.closed {
		return nil
	}
	for _, ic := range pool.idleConns {
		if err := ic.conn.Close(); err != nil {
			return err
		}
	}
	pool.closed = true
	pool.idleConns = nil
	return nil
}
