package gomemcache

// protocol doc(https://github.com/memcached/memcached/blob/master/doc/protocol.txt)

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
	"sync"
)

type TextProtocol struct {
	baseProtocol
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
	var index uint32
	if protocol.poolSize != 1 {
		index = protocol.getPoolIndex(item.Key)
	}
	pool := protocol.pools[index]
	conn, err := pool.Get()
	if err != nil {
		return err
	}
	if _, err = conn.Write(buf); err != nil {
		conn.SetError(err)
		pool.Put(conn)
		return err
	}
	if op.quiet {
		pool.Put(conn)
		return nil
	}
	// 12 is the max bytes size read from server
	b := make([]byte, 12)
	n, err := conn.Read(b)
	err = protocol.checkError(b[:n], err)
	if err == ErrOperationNotSupported {
		conn.SetError(err)
	}
	pool.Put(conn)
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

func (protocol TextProtocol) fetch(keys []string, withCAS bool) ([]*Item, error) {
	if protocol.poolSize == 1 {
		return protocol.fetchFromServer(0, keys, withCAS)
	}
	array := make([][]string, protocol.poolSize)
	for _, key := range keys {
		index := protocol.getPoolIndex(key)
		array[index] = append(array[index], key)
	}
	var wg sync.WaitGroup
	var err error
	results := make([]*Item, 0, len(keys))
	for index, ks := range array {
		if ks == nil {
			continue
		}
		wg.Add(1)
		go func(idx int, iks []string, cas bool) {
			result, e := protocol.fetchFromServer(idx, iks, cas)
			if e != nil {
				err = e
			}
			if len(result) != 0 {
				results = append(results, result...)
			}
			wg.Done()
		}(index, ks, withCAS)
	}
	wg.Wait()
	return results, err
}

func (protocol TextProtocol) fetchFromServer(index int, keys []string, withCAS bool) ([]*Item, error) {
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
	pool := protocol.pools[index]
	conn, err := pool.Get()
	if err != nil {
		return nil, err
	}
	_, err = conn.Write(buf)
	if err != nil {
		conn.SetError(err)
		pool.Put(conn)
		return nil, err
	}
	result := make([]*Item, 0, len(keys))
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadSlice('\n')
		if err != nil {
			conn.SetError(err)
			pool.Put(conn)
			return nil, err
		}
		if bytes.Equal(line, []byte("END\r\n")) {
			pool.Put(conn)
			return result, nil
		}
		var key string
		var cas uint64
		var num, size, flags int
		line = line[6 : len(line)-2]
		for idx, row := range line {
			if row == 32 || row == 13 {
				if num == 0 {
					key = string(line[0:idx])
				}
				num += 1
			} else if num == 1 {
				flags = flags*10 + int(row-48)
			} else if num == 2 {
				size = size*10 + int(row-48)
			} else if num == 3 {
				cas = cas*10 + uint64(row-48)
			}
		}
		value := make([]byte, size+2)
		// include the delimiter \r\n
		n, err := io.ReadFull(reader, value)
		if err != nil || n != size+2 {
			conn.SetError(err)
			pool.Put(conn)
			return nil, err
		}
		item := &Item{Key: key, Value: value[:size], Flags: uint32(flags), CAS: cas}
		result = append(result, item)
	}
}
