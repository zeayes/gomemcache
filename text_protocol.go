package gomemcache

// protocol doc(https://github.com/memcached/memcached/blob/master/doc/protocol.txt)

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

const defaultItemValueSize = 1024

type TextProtocol struct {
	pool *Pool
}

func (protocol TextProtocol) setMaxIdleConns(maxIdleConns int) {
	protocol.pool.MaxIdleConns = maxIdleConns
}

func (protocol TextProtocol) setMaxActiveConns(maxActiveConns int) {
	protocol.pool.MaxActiveConns = maxActiveConns
}

func (protocol TextProtocol) setIdleTimeout(timeout time.Duration) {
	if timeout < protocol.pool.SocketTimeout {
		timeout = protocol.pool.SocketTimeout
	}
	protocol.pool.IdleTimeout = timeout
}

func (protocol TextProtocol) setSocketTimeout(timeout time.Duration) {
	protocol.pool.SocketTimeout = timeout
}

func (protocol TextProtocol) store(cmd string, item *Item) error {
	op, ok := operations[cmd]
	if !ok {
		return ErrOperationNotSupported
	}
	isStored := isStoreOperation(op)
	buf := make([]byte, 0)
	buf = append(buf, op.command...)
	buf = append(buf, ' ')
	buf = append(buf, item.Key...)
	buf = append(buf, ' ')
	if isStored {
		buf = append(buf, strconv.FormatUint(uint64(item.Flags), 10)...)
		buf = append(buf, ' ')
		buf = append(buf, strconv.FormatUint(uint64(item.Expiration), 10)...)
		buf = append(buf, ' ')
		buf = append(buf, strconv.Itoa(len(item.Value))...)
		buf = append(buf, ' ')
		if op.command == "cas" {
			buf = append(buf, strconv.FormatUint(item.CAS, 10)...)
			buf = append(buf, ' ')
		}
	} else if op.command == "incr" || op.command == "decr" {
		buf = append(buf, strconv.Itoa(len(item.Value))...)
		buf = append(buf, ' ')
	}
	if op.quiet {
		buf = append(buf, "noreply"...)
	}
	buf = append(buf, "\r\n"...)
	if isStored {
		buf = append(buf, item.Value...)
		buf = append(buf, "\r\n"...)
	}
	conn, err := protocol.pool.Get()
	if err != nil {
		return err
	}
	if _, err = conn.Write(buf); err != nil {
		conn.Close()
		return err
	}
	if op.quiet {
		protocol.pool.Put(conn)
		return nil
	}
	// 12 is the max bytes size read from server
	b := make([]byte, 12)
	n, err := conn.Read(b)
	err = protocol.checkError(b[:n], err)
	if err == ErrOperationNotSupported {
		conn.Close()
	} else {
		protocol.pool.Put(conn)
	}
	return err
}

func (protocol TextProtocol) checkError(buf []byte, err error) error {
	if err != nil {
		return err
	}
	if bytes.Equal(buf, []byte("STORED\r\n")) {
		return nil
	}
	if bytes.Equal(buf, []byte("NOT_STORED\r\n")) {
		return ErrItemNotStored
	}
	if bytes.Equal(buf, []byte("EXISTS\r\n")) {
		return ErrItemExists
	}
	if bytes.Equal(buf, []byte("NOT_FOUND\r\n")) {
		return ErrItemNotFound
	}
	if bytes.Equal(buf, []byte("DELETED\r\n")) {
		return nil
	}
	return fmt.Errorf("server response error %s doesn't define", string(buf))
}

func (protocol TextProtocol) fetch(keys []string, withCAS bool) (map[string]*Item, error) {
	var cmd []byte
	if withCAS {
		cmd = []byte("gets")
	} else {
		cmd = []byte("get")
	}
	count := len(keys)
	length := count + len(cmd)
	for i := 0; i < count; i++ {
		length += len(keys[i])
	}
	buf := make([]byte, 0, length+2)
	buf = append(buf, cmd...)
	for _, key := range keys {
		buf = append(buf, ' ')
		buf = append(buf, []byte(key)...)
	}
	buf = append(buf, '\r')
	buf = append(buf, '\n')
	conn, err := protocol.pool.Get()
	if err != nil {
		return nil, err
	}
	_, err = conn.Write(buf)
	if err != nil {
		conn.Close()
		return nil, err
	}
	result := make(map[string]*Item, len(keys))
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadSlice('\n')
		if err != nil {
			conn.Close()
			return nil, err
		}
		if bytes.Equal(line, []byte("END\r\n")) {
			protocol.pool.Put(conn)
			return result, nil
		}
		values := strings.Split(string(line[6:len(line)-2]), " ")
		num := len(values)
		if num != 3 && num != 4 {
			return nil, ErrInvalidResponseFormat
		}
		flags, err := strconv.ParseUint(values[1], 10, 32)
		if err != nil {
			conn.Close()
			return nil, err
		}
		size, err := strconv.ParseUint(values[2], 10, 32)
		if err != nil {
			conn.Close()
			return nil, err
		}
		value := make([]byte, size+2)
		// include the delimiter \r\n
		n, err := io.ReadFull(reader, value)
		if err != nil || uint64(n) != size+2 {
			conn.Close()
			return nil, err
		}
		item := &Item{Key: values[0], Value: value[:size], Flags: uint32(flags)}
		if num == 4 {
			if item.CAS, err = strconv.ParseUint(values[3], 10, 64); err != nil {
				conn.Close()
				return nil, err
			}
		}
		result[item.Key] = item
	}
}
