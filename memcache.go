package gomemcache

import (
	"errors"
	"fmt"
	"net"
	"time"
)

const (
	defaultMaxIdleConns  = 10
	defaultIdleTimeout   = 60 * time.Second
	defaultSocketTimeout = 100 * time.Millisecond
)

var (
	// ErrItemNotFound indicates the item was not found when command is cas/delete/incr/decr
	ErrItemNotFound = errors.New("item is not found")
	// ErrItemExists indicates the item has stored where command is cas
	ErrItemExists = errors.New("item exists")
	// ErrItemNotStored indicates the data not stored where command is add
	ErrItemNotStored = errors.New("item is not stored")
	// ErrOperationNotSupported indicates that you send an unkonwn command
	ErrOperationNotSupported = errors.New("operation is not supported")
)

// Item item stored in memcache server
type Item struct {
	Key        string
	Value      []byte
	Expiration uint32
	Flags      uint32
	CAS        uint64
}

// Protocol (binary or text) supported by memcached should implements interface
type Protocol interface {
	setMaxIdleConns(maxIdleConns int)
	setIdleTimeout(timeout time.Duration)
	setSocketTimeout(timeout time.Duration)
	store(command string, item *Item) error
	fetch(keys []string) (map[string]*Item, error)
}

// Client memcache client for writing and reading
type Client struct {
	server   string
	protocol Protocol
	noreply  bool
}

// NewClient create memcache client
func NewClient(server string) (*Client, error) {
	pool := Pool{
		DialFunc: func() (Conn, error) {
			conn, err := net.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			return conn, err
		},
	}
	pool.MaxIdleConns = defaultMaxIdleConns
	pool.IdleTimeout = defaultIdleTimeout
	pool.SocketTimeout = defaultSocketTimeout
	return &Client{server: server, protocol: TextProtocol{pool: &pool}, noreply: true}, nil
}

// SetMaxIdleConns set max idle connections
func (client *Client) SetMaxIdleConns(maxIdleConns int) {
	client.protocol.setMaxIdleConns(maxIdleConns)
}

// SetIdleTiemout set connection idle timeout
func (client *Client) SetIdleTiemout(timeout time.Duration) {
	client.protocol.setIdleTimeout(timeout)
}

// SetSocketTimeout set connection operation timeout
func (client *Client) SetSocketTimeout(timeout time.Duration) {
	client.protocol.setSocketTimeout(timeout)
}

// SetProtocol set the default protocol, it's TextProtocol or BinaryProtocol.
func (client *Client) SetProtocol(protocol string) error {
	if protocol != "text" && protocol != "binary" {
		return fmt.Errorf("only support 'text' and 'binary' protocol")
	}
	pool := Pool{
		DialFunc: func() (Conn, error) {
			conn, err := net.Dial("tcp", client.server)
			if err != nil {
				return nil, err
			}
			return conn, err
		},
	}
	pool.MaxIdleConns = defaultMaxIdleConns
	pool.IdleTimeout = defaultIdleTimeout
	pool.SocketTimeout = defaultSocketTimeout
	if protocol == "text" {
		client.protocol = TextProtocol{pool: &pool}
	} else {
		client.protocol = BinaryProtocol{pool: &pool}
	}
	return nil
}

// SetNoreply set command noreply option
// It's avialable for *Set* *Delete*.
func (client *Client) SetNoreply(noreply bool) {
	client.noreply = noreply
}

// Set store this item
func (client *Client) Set(item *Item) error {
	cmd := "setq"
	if !client.noreply {
		cmd = "set"
	}
	return client.protocol.store(cmd, item)
}

// Add store this data, but only if the server
// *doesn't* already hold data for this key
func (client *Client) Add(item *Item) error {
	return client.protocol.store("add", item)
}

// CAS store this item but only if no one
// else has updated since I last fetched it
func (client *Client) CAS(item *Item) error {
	return client.protocol.store("cas", item)
}

// Replace store this data, but only if the
// server *does* already hold data for this key
func (client *Client) Replace(item *Item) error {
	return client.protocol.store("replace", item)
}

// Get retrieve an item from the server with a key.
func (client *Client) Get(key string) (*Item, error) {
	items, err := client.protocol.fetch([]string{key})
	if err != nil {
		return nil, err
	}
	if item, ok := items[key]; ok {
		return item, nil
	}
	return nil, nil
}

// MultiGet retrieve bulk items with some keys
func (client *Client) MultiGet(keys []string) (map[string]*Item, error) {
	ks := keys[:0]
	for _, key := range keys {
		exists := false
		for _, k := range ks {
			if k == key {
				exists = true
			}
		}
		if !exists {
			ks = append(ks, key)
		}
	}
	return client.protocol.fetch(ks)
}

// Delete explicit deletion of items
func (client *Client) Delete(key string) error {
	cmd := "deleteq"
	if !client.noreply {
		cmd = "delete"
	}
	return client.protocol.store(cmd, &Item{Key: key})
}
