package gomemcache

// protocol doc(https://github.com/memcached/memcached/blob/master/doc/protocol.txt)

import (
	"bufio"
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type TextProtocol struct {
	pool *Pool
}

func (protocol TextProtocol) setMaxIdleConns(maxIdleConns int) {
	protocol.pool.MaxIdleConns = maxIdleConns
}

func (protocol TextProtocol) setIdleTimeout(timeout time.Duration) {
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

func (protocol TextProtocol) fetch(keys []string) (map[string]*Item, error) {
	count := len(keys)
	length := count
	for i := 0; i < count; i++ {
		length += len(keys[i])
	}
	buf := make([]byte, 0, length+7)
	buf = append(buf, "gets "...)
	buf = append(buf, keys[0]...)
	for i := 1; i < count; i++ {
		buf = append(buf, ' ')
		buf = append(buf, keys[i]...)
	}
	buf = append(buf, "\r\n"...)
	conn, err := protocol.pool.Get()
	_, err = conn.Write(buf)
	if err != nil {
		conn.Close()
		return nil, err
	}
	result := make(map[string]*Item, len(keys))
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		text := scanner.Text()
		if text == "END" {
			break
		}
		values := strings.Split(scanner.Text(), " ")
		if len(values) >= 4 {
			scanner.Scan()
			flags, err := strconv.ParseUint(values[2], 10, 32)
			if err != nil {
				conn.Close()
				return nil, err
			}
			item := &Item{Key: values[1], Value: scanner.Bytes(), Flags: uint32(flags)}
			if len(values) == 5 {
				if item.CAS, err = strconv.ParseUint(values[4], 10, 64); err != nil {
					conn.Close()
					return nil, err
				}
			}
			result[item.Key] = item
		}
	}
	if err := scanner.Err(); err != nil {
		conn.Close()
		return nil, err
	}
	protocol.pool.Put(conn)
	return result, nil
}
