package gomemcache

import (
	"errors"
	"fmt"
	"hash/crc32"
	"net"
	"time"
)

const (
	defaultMaxIdleConns   = 10
	defaultMaxActiveConns = 20
	defaultIdleTimeout    = 60 * time.Second
	defaultSocketTimeout  = 100 * time.Millisecond
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
	// ErrInvalidResponseFormat suggests that the response can't be parsed.
	ErrInvalidResponseFormat = errors.New("The server repsonse error value format")
	// ErrInvalidKey indicates the key is invalid.
	ErrInvalidKey = errors.New("invalid key, key must be less than 250 and can't contain black or control character")
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
	setMaxActiveConns(maxActiveConns int)
	setIdleTimeout(timeout time.Duration)
	setSocketTimeout(timeout time.Duration)
	store(command string, item *Item) error
	fetch(keys []string, withCAS bool) (map[string]*Item, error)
}

type baseProtocol struct {
	pools    []*Pool
	poolSize uint32
	hashFunc func(buf []byte) uint32
}

func (protocol baseProtocol) getPoolIndex(key string) uint32 {
	return protocol.hashFunc([]byte(key)) % protocol.poolSize
}

func (protocol baseProtocol) setMaxIdleConns(maxIdleConns int) {
	for _, pool := range protocol.pools {
		pool.MaxIdleConns = maxIdleConns
	}
}

func (protocol baseProtocol) setMaxActiveConns(maxActiveConns int) {
	for _, pool := range protocol.pools {
		pool.MaxActiveConns = maxActiveConns
	}
}

func (protocol baseProtocol) setIdleTimeout(timeout time.Duration) {
	for _, pool := range protocol.pools {
		if timeout < pool.SocketTimeout {
			timeout = pool.SocketTimeout
		}
		pool.IdleTimeout = timeout
	}
}

func (protocol baseProtocol) setSocketTimeout(timeout time.Duration) {
	for _, pool := range protocol.pools {
		pool.SocketTimeout = timeout
	}
}

// Client memcache client for writing and reading
type Client struct {
	servers  []string
	protocol Protocol
	noreply  bool
}

func invalidKey(key string) bool {
	// key must be less than 250 and can't contain black and control character
	length := len(key)
	if length > 250 {
		return false
	}
	for i := 0; i < length; i++ {
		if key[i] <= ' ' || key[i] > 0x7f {
			return false
		}
	}
	return true
}

// NewClient create memcache client
func NewClient(servers []string) (*Client, error) {
	client := &Client{servers: servers, noreply: true}
	err := client.SetProtocol("text")
	return client, err
}

// SetMaxIdleConns set max idle connections
func (client *Client) SetMaxIdleConns(maxIdleConns int) {
	client.protocol.setMaxIdleConns(maxIdleConns)
}

func (client *Client) SetMaxActiveConns(maxActiveConns int) {
	client.protocol.setMaxActiveConns(maxActiveConns)
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
	poolSize := len(client.servers)
	pools := make([]*Pool, 0, poolSize)
	for _, server := range client.servers {
		pool := Pool{
			DialFunc: func() (Conn, error) {
				conn, err := net.Dial("tcp", server)
				if err != nil {
					return nil, err
				}
				return conn, err
			},
			IdleTimeout:    defaultIdleTimeout,
			SocketTimeout:  defaultSocketTimeout,
			MaxIdleConns:   defaultMaxIdleConns,
			MaxActiveConns: defaultMaxActiveConns,
		}
		pools = append(pools, &pool)
	}
	base := baseProtocol{
		pools:    pools,
		poolSize: uint32(poolSize),
		hashFunc: func(buf []byte) uint32 {
			return (((crc32.ChecksumIEEE(buf) & 0xffffffff) >> 16) & 0x7fff) | 1
		},
	}
	if protocol == "text" {
		client.protocol = TextProtocol{base}
	} else {
		client.protocol = BinaryProtocol{base}
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
	if !invalidKey(item.Key) {
		return ErrInvalidKey
	}
	cmd := "setq"
	if !client.noreply {
		cmd = "set"
	}
	return client.protocol.store(cmd, item)
}

// Add store this data, but only if the server
// *doesn't* already hold data for this key
func (client *Client) Add(item *Item) error {
	if !invalidKey(item.Key) {
		return ErrInvalidKey
	}
	return client.protocol.store("add", item)
}

// CAS store this item but only if no one
// else has updated since I last fetched it
func (client *Client) CAS(item *Item) error {
	if !invalidKey(item.Key) {
		return ErrInvalidKey
	}
	return client.protocol.store("cas", item)
}

// Replace store this data, but only if the
// server *does* already hold data for this key
func (client *Client) Replace(item *Item) error {
	if !invalidKey(item.Key) {
		return ErrInvalidKey
	}
	return client.protocol.store("replace", item)
}

// Gets retrieve an item from the server with a key, Item responses with CAS
func (client *Client) Gets(key string) (*Item, error) {
	if !invalidKey(key) {
		return nil, ErrInvalidKey
	}
	items, err := client.protocol.fetch([]string{key}, true)
	if err != nil {
		return nil, err
	}
	if item, ok := items[key]; ok {
		return item, nil
	}
	return nil, nil
}

// Get retrieve an item from the server with a key.
func (client *Client) Get(key string) (*Item, error) {
	if !invalidKey(key) {
		return nil, ErrInvalidKey
	}
	items, err := client.protocol.fetch([]string{key}, false)
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
			if !invalidKey(key) {
				return nil, ErrInvalidKey
			}
			ks = append(ks, key)
		}
	}
	if len(ks) == 0 {
		return nil, nil
	}
	return client.protocol.fetch(ks, false)
}

// Delete explicit deletion of items
func (client *Client) Delete(key string) error {
	if !invalidKey(key) {
		return ErrInvalidKey
	}
	cmd := "deleteq"
	if !client.noreply {
		cmd = "delete"
	}
	return client.protocol.store(cmd, &Item{Key: key})
}
