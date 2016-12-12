package gomemcache

// protocol doc(https://github.com/memcached/memcached/blob/master/doc/protocol.txt)

import (
	"bufio"
	"bytes"
	"errors"
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
	dst := []string{op.command, item.Key}
	isStored := isStoreOperation(op)
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

func (protocol TextProtocol) parseResponseLine(line []byte) (*Item, int, error) {
	// strip \r\n
	if !bytes.Equal(line[:5], []byte("VALUE")) || !bytes.Equal(line[len(line)-2:], []byte("\r\n")) {
		return nil, 0, errors.New("The server responses error value format")
	}
	values := bytes.Split(line[:len(line)-2], []byte(" "))
	length := len(values)
	if length != 4 && length != 5 {
		return nil, 0, errors.New("The server responses error value format")
	}
	size, err := strconv.Atoi(string(values[3]))
	if err != nil {
		return nil, 0, err
	}
	flags, err := strconv.ParseUint(string(values[2]), 10, 32)
	if err != nil {
		return nil, 0, err
	}
	item := &Item{Key: string(values[1]), Flags: uint32(flags)}
	item.Key = string(values[1])
	if length == 5 {
		if item.CAS, err = strconv.ParseUint(string(values[4]), 10, 64); err != nil {
			return nil, 0, err
		}
	}
	return item, size, nil
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
		item, size, err := protocol.parseResponseLine(line)
		if err != nil {
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
