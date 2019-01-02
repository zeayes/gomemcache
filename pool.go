package gomemcache

import (
	"errors"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	nowFunc = time.Now
	// ErrPoolExhausted idle connection pool exhausted
	ErrPoolExhausted = errors.New("connection pool exhausted")
	errPoolClosed    = errors.New("pool is closed ")
	// https://github.com/valyala/fasthttp/blob/master/coarseTime.go
	coarseTime atomic.Value
)

// Conn connection used in pool
type Conn net.Conn

// Pool goroutine safe connection pool
type Pool struct {
	DialFunc       func() (Conn, error)
	MaxIdleConns   int
	MaxActiveConns int
	IdleTimeout    time.Duration
	SocketTimeout  time.Duration

	mu          sync.Mutex
	closed      bool
	idleConns   []*idleConn // idle connections list, latest connection appending the last
	activeConns int
}

// Conn net connection with idle timeout
type idleConn struct {
	Conn
	err    error
	idleAt time.Time
}

func (conn *idleConn) SetError(err error) {
	conn.err = err
}

func (conn *idleConn) CheckError() bool {
	return conn.err != nil
}

// Get get a connection from idle conns
func (pool *Pool) Get() (*idleConn, error) {
	if pool.closed {
		return nil, errPoolClosed
	}
	if pool.activeConns > pool.MaxActiveConns {
		log.Printf("max active conns: %d, current active conns: %d, current idle conns: %d",
			pool.MaxActiveConns, pool.activeConns, len(pool.idleConns))
		return nil, ErrPoolExhausted
	}
	pool.mu.Lock()
	expiredSince := nowFunc().Add(-pool.IdleTimeout)
	index := len(pool.idleConns)
	for idx, ic := range pool.idleConns {
		// find the first active connection, behind connections must be active.
		if ic.idleAt.After(expiredSince) {
			index = idx
			break
		}
		pool.idleConns[idx] = nil
		// close expired connection
		if err := ic.Close(); err != nil {
			pool.mu.Unlock()
			return nil, err
		}
	}
	pool.idleConns = pool.idleConns[index:]
	numIdle := len(pool.idleConns)
	if numIdle == 0 {
		c, err := pool.DialFunc()
		if err != nil {
			pool.mu.Unlock()
			return nil, err
		}
		if err = c.SetDeadline(nowFunc().Add(pool.SocketTimeout)); err != nil {
			pool.mu.Unlock()
			return nil, err
		}
		pool.activeConns++
		log.Printf("after create new client, current active conns: %d", pool.activeConns)
		pool.mu.Unlock()
		return &idleConn{Conn: c}, nil
	}
	pool.activeConns++
	conn := pool.idleConns[numIdle-1]
	pool.idleConns[numIdle-1] = nil
	pool.idleConns = pool.idleConns[:numIdle-1]
	pool.mu.Unlock()
	if err := conn.SetDeadline(nowFunc().Add(pool.SocketTimeout)); err != nil {
		return nil, err
	}
	return conn, nil
}

// Put put an idle conn into idle conns
func (pool *Pool) Put(ic *idleConn) error {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	pool.activeConns--
	if pool.closed || len(pool.idleConns) >= pool.MaxIdleConns || ic.CheckError() {
		return ic.Close()
	}
	ic.idleAt = nowFunc()
	pool.idleConns = append(pool.idleConns, ic)
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
		pool.activeConns--
		if err := ic.Close(); err != nil {
			return err
		}
	}
	pool.closed = true
	pool.idleConns = nil
	return nil
}
