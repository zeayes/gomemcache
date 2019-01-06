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

const (
	getCmd  = "get"
	getsCmd = "gets"
	casCmd  = "cas"
	incrCmd = "incr"
	decrCmd = "decr"

	zeroDelimiter     = '0'
	spaceDelimiter    = ' '
	carriageDelimiter = '\r'
	newlineDelimiter  = '\n'
)

var (
	noReplyDelimiter = []byte("noreply")

	endDelimiter       = []byte("END\r\n")
	existsDelimiter    = []byte("EXISTS\r\n")
	storedDelimiter    = []byte("STORED\r\n")
	deletedDelimiter   = []byte("DELETED\r\n")
	notFoundDelimiter  = []byte("NOT_FOUND\r\n")
	notStoredDelimiter = []byte("NOT_STORED\r\n")
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
	buf = append(buf, spaceDelimiter)
	buf = append(buf, item.Key...)
	buf = append(buf, spaceDelimiter)
	if isStored {
		buf = append(buf, strconv.FormatUint(uint64(item.Flags), 10)...)
		buf = append(buf, spaceDelimiter)
		buf = append(buf, strconv.FormatUint(uint64(item.Expiration), 10)...)
		buf = append(buf, spaceDelimiter)
		buf = append(buf, strconv.Itoa(len(item.Value))...)
		buf = append(buf, spaceDelimiter)
		if op.command == casCmd {
			buf = append(buf, strconv.FormatUint(item.CAS, 10)...)
			buf = append(buf, spaceDelimiter)
		}
	} else if op.command == incrCmd || op.command == decrCmd {
		buf = append(buf, strconv.Itoa(len(item.Value))...)
		buf = append(buf, spaceDelimiter)
	}
	if op.quiet {
		buf = append(buf, noReplyDelimiter...)
	}
	buf = append(buf, carriageDelimiter, newlineDelimiter)
	if isStored {
		buf = append(buf, item.Value...)
		buf = append(buf, carriageDelimiter, newlineDelimiter)
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
	if bytes.Equal(buf, storedDelimiter) {
		return nil
	}
	if bytes.Equal(buf, notStoredDelimiter) {
		return ErrItemNotStored
	}
	if bytes.Equal(buf, existsDelimiter) {
		return ErrItemExists
	}
	if bytes.Equal(buf, notFoundDelimiter) {
		return ErrItemNotFound
	}
	if bytes.Equal(buf, deletedDelimiter) {
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
	var cmd string
	if withCAS {
		cmd = getsCmd
	} else {
		cmd = getCmd
	}
	count := len(keys)
	length := count + len(cmd)
	for i := 0; i < count; i++ {
		length += len(keys[i])
	}
	buf := make([]byte, 0, length+2)
	buf = append(buf, cmd...)
	for _, key := range keys {
		buf = append(buf, spaceDelimiter)
		buf = append(buf, key...)
	}
	buf = append(buf, carriageDelimiter, newlineDelimiter)
	pool := protocol.pools[index]
	conn, err := pool.Get()
	if err != nil {
		return nil, err
	}
	var total int
	for {
		c, err := conn.Write(buf[total:])
		if err != nil {
			conn.SetError(err)
			pool.Put(conn)
			return nil, err
		}
		total += c
		if total == len(buf) {
			break
		}
	}
	result := make([]*Item, 0, len(keys))
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadSlice(newlineDelimiter)
		if err != nil {
			conn.SetError(err)
			pool.Put(conn)
			return nil, err
		}
		if bytes.Equal(line, endDelimiter) {
			pool.Put(conn)
			return result, nil
		}
		var key string
		var cas uint64
		var num, size, flags int
		// line contains "VALUE <key> <flags> <bytes> [<cas unique>]\r\n"
		for idx, row := range line[6 : len(line)-2] {
			if row == spaceDelimiter || row == carriageDelimiter {
				if num == 0 {
					key = string(line[6 : 6+idx])
				}
				num++
			} else if num == 1 {
				flags = flags*10 + int(row-zeroDelimiter)
			} else if num == 2 {
				size = size*10 + int(row-zeroDelimiter)
			} else if num == 3 {
				cas = cas*10 + uint64(row-zeroDelimiter)
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
