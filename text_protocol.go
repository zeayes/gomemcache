package gomemcache

// protocol doc(https://github.com/memcached/memcached/blob/master/doc/protocol.txt)

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
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
	isStored := op.command == "set" || op.command == "add" || op.command == "replace" || op.command == "cas"
	dst := []string{op.command, item.Key}
	if isStored {
		dst = append(dst, strconv.Itoa(int(item.Flags)))
		dst = append(dst, strconv.Itoa(int(item.Expiration)))
		dst = append(dst, strconv.Itoa(len(item.Value)))
	}
	if op.command == "cas" {
		dst = append(dst, strconv.Itoa(int(item.CAS)))
	}
	if op.command == "incr" || op.command == "decr" {
		dst = append(dst, string(item.Value))
	}
	if op.quiet {
		dst = append(dst, "noreply")
	}
	src := strings.Join(dst, " ")
	cmdBuf := make([]byte, 0, len(src)+len(item.Value)+4)
	buffer := bytes.NewBuffer(cmdBuf)
	buffer.WriteString(src)
	buffer.WriteString("\r\n")
	if isStored {
		buffer.Write(item.Value)
		buffer.WriteString("\r\n")
	}
	conn, err := protocol.pool.Get()
	if _, err = buffer.WriteTo(conn); err != nil {
		conn.Close()
		return err
	}
	if op.quiet {
		protocol.pool.Put(conn)
		return nil
	}
	// 12 is the max bytes size read from server
	buf := make([]byte, 12)
	n, err := conn.Read(buf)
	err = protocol.checkError(buf[:n], err)
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
	buffer := new(bytes.Buffer)
	buffer.WriteString("gets ")
	buffer.WriteString(strings.Join(keys, " "))
	buffer.WriteString("\r\n")
	conn, err := protocol.pool.Get()
	_, err = buffer.WriteTo(conn)
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
		pattern := "VALUE %s %d %d %d\r\n"
		item := new(Item)
		var size int
		dest := []interface{}{&item.Key, &item.Flags, &size, &item.CAS}
		if bytes.Count(line, []byte(" ")) == 3 {
			pattern = "VALUE %s %d %d\r\n"
			dest = dest[:3]
		}
		n, err := fmt.Sscanf(string(line), pattern, dest...)
		if err != nil || n != len(dest) {
			conn.Close()
			return nil, err
		}
		value, err := ioutil.ReadAll(io.LimitReader(reader, int64(size)+2))
		if err != nil {
			conn.Close()
			return nil, err
		}
		item.Value = value[:size]
		result[item.Key] = item
	}
}
