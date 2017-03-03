package gomemcache

// implements this protocol with the help of official
// doc(https://github.com/memcached/memcached/wiki/BinaryProtocolRevamped)

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
)

const (
	requestMagic  = 0x80
	responseMagic = 0x81
)

var (
	hdrSize = binary.Size(header{})

	errorMap = map[uint16]error{
		0x001: ErrItemNotFound,
		0x002: ErrItemExists,
		0x003: errors.New("Value too large"),
		0x004: errors.New("Invalid arguments"),
		0x005: ErrItemNotStored,
		0x006: errors.New("Incr/Decr on non-numeric value"),
		0x007: errors.New("The vbucket belongs to another server"),
		0x008: errors.New("Authentication error"),
		0x009: errors.New("Authentication continue"),
		0x081: errors.New("Unknown command"),
		0x082: errors.New("Out of memory"),
		0x083: errors.New("Not supported"),
		0x084: errors.New("Internal error"),
		0x085: errors.New("Busy"),
		0x086: errors.New("Temporary failure"),
	}

	operations = map[string]operation{
		"get":        operation{opcode: 0x00, command: "get", quiet: false, withKey: false},
		"set":        operation{opcode: 0x01, command: "set", quiet: false, withKey: false},
		"add":        operation{opcode: 0x02, command: "add", quiet: false, withKey: false},
		"replace":    operation{opcode: 0x03, command: "replace", quiet: false, withKey: false},
		"delete":     operation{opcode: 0x04, command: "delete", quiet: false, withKey: false},
		"increment":  operation{opcode: 0x05, command: "incr", quiet: false, withKey: false},
		"decrement":  operation{opcode: 0x06, command: "decr", quiet: false, withKey: false},
		"quit":       operation{opcode: 0x07, command: "quit", quiet: false, withKey: false},
		"flush":      operation{opcode: 0x08, command: "flush", quiet: false, withKey: false},
		"getq":       operation{opcode: 0x09, command: "get", quiet: true, withKey: false},
		"noop":       operation{opcode: 0x0a, command: "noop", quiet: false, withKey: false},
		"version":    operation{opcode: 0x0b, command: "version", quiet: false, withKey: false},
		"getk":       operation{opcode: 0x0c, command: "get", quiet: false, withKey: true},
		"getkq":      operation{opcode: 0x0d, command: "get", quiet: true, withKey: false},
		"append":     operation{opcode: 0x0e, command: "append", quiet: false, withKey: false},
		"prepend":    operation{opcode: 0x0f, command: "prepend", quiet: false, withKey: false},
		"stat":       operation{opcode: 0x10, command: "stat", quiet: false, withKey: false},
		"setq":       operation{opcode: 0x11, command: "set", quiet: true, withKey: false},
		"addq":       operation{opcode: 0x12, command: "add", quiet: true, withKey: false},
		"replaceq":   operation{opcode: 0x13, command: "replace", quiet: true, withKey: false},
		"deleteq":    operation{opcode: 0x14, command: "delete", quiet: true, withKey: false},
		"incrementq": operation{opcode: 0x15, command: "incr", quiet: true, withKey: false},
		"decrementq": operation{opcode: 0x16, command: "decr", quiet: true, withKey: false},
		"quitq":      operation{opcode: 0x17, command: "quit", quiet: true, withKey: false},
		"flushq":     operation{opcode: 0x18, command: "flush", quiet: true, withKey: false},
		"appendq":    operation{opcode: 0x19, command: "append", quiet: true, withKey: false},
		"prependq":   operation{opcode: 0x1a, command: "prepend", quiet: true, withKey: false},
		"cas":        operation{opcode: 0x01, command: "cas", quiet: false, withKey: false},
		// memcached binary protocol doesn't define this operation (cas)
	}
)

type operation struct {
	opcode  uint8
	command string
	quiet   bool
	withKey bool
}

func isStoreOperation(op operation) bool {
	return op.command == "set" || op.command == "add" || op.command == "replace" || op.command == "cas"
}

// Header for request and response
type header struct {
	magic        uint8
	opcode       uint8
	keyLength    uint16
	extrasLength uint8
	dataType     uint8
	status       uint16
	bodyLength   uint32
	opaque       uint32
	cas          uint64
}

func (hdr *header) read(reader io.Reader) error {
	hdrBuf := make([]byte, hdrSize)
	if n, err := io.ReadFull(reader, hdrBuf); err != nil || n != hdrSize {
		return err
	}
	hdr.magic = hdrBuf[0]
	hdr.opcode = hdrBuf[1]
	hdr.keyLength = binary.BigEndian.Uint16(hdrBuf[2:4])
	hdr.extrasLength = hdrBuf[4]
	hdr.dataType = hdrBuf[5]
	hdr.status = binary.BigEndian.Uint16(hdrBuf[6:8])
	hdr.bodyLength = binary.BigEndian.Uint32(hdrBuf[8:12])
	hdr.opaque = binary.BigEndian.Uint32(hdrBuf[12:16])
	hdr.cas = binary.BigEndian.Uint64(hdrBuf[16:24])
	return nil
}

func (hdr *header) write(buf []byte) {
	buf[0] = hdr.magic
	buf[1] = hdr.opcode
	binary.BigEndian.PutUint16(buf[2:4], hdr.keyLength)
	buf[4] = hdr.extrasLength
	buf[5] = hdr.dataType
	binary.BigEndian.PutUint16(buf[6:8], hdr.status)
	binary.BigEndian.PutUint32(buf[8:12], hdr.bodyLength)
	binary.BigEndian.PutUint32(buf[12:16], hdr.opaque)
	binary.BigEndian.PutUint64(buf[16:24], hdr.cas)
}

// Packet for request and response
type packet struct {
	header
	extras []byte
	key    string
	value  []byte
}

func (pkt *packet) write(writer io.Writer) error {
	buf := make([]byte, hdrSize, uint32(hdrSize)+pkt.bodyLength)
	// if err := binary.Write(buffer, binary.BigEndian, pkt.header); err != nil {
	// return err
	// }
	pkt.header.write(buf)
	buf = append(buf, pkt.extras...)
	buf = append(buf, pkt.key...)
	buf = append(buf, pkt.value...)
	_, err := writer.Write(buf)
	return err
}

func (pkt *packet) read(reader io.Reader) error {
	if err := pkt.header.read(reader); err != nil {
		return err
	}
	// if err := binary.Read(reader, binary.BigEndian, &pkt.header); err != nil {
	// return err
	// }
	body := make([]byte, pkt.bodyLength)
	if n, err := io.ReadFull(reader, body); err != nil || uint32(n) != pkt.bodyLength {
		return err
	}
	keyOffset := uint16(pkt.extrasLength) + pkt.keyLength
	if pkt.keyLength != 0 {
		pkt.key = string(body[pkt.extrasLength:keyOffset])
	}
	if pkt.bodyLength-uint32(keyOffset) != 0 {
		pkt.value = body[keyOffset:]
	}
	if pkt.extrasLength != 0 {
		pkt.extras = body[:pkt.extrasLength]
	}
	if pkt.status == 0 {
		return nil
	}
	e, ok := errorMap[pkt.status]
	if ok {
		return e
	}
	return fmt.Errorf("server response status code error: %d", pkt.status)
}

// BinaryProtocol implements binary protocol
type BinaryProtocol struct {
	baseProtocol
}

func (protocol BinaryProtocol) fetch(keys []string, withCAS bool) (map[string]*Item, error) {
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
	results := make(map[string]*Item, len(keys))
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
			for k, v := range result {
				results[k] = v
			}
			wg.Done()
		}(index, ks, withCAS)
	}
	wg.Wait()
	return results, err
}

func (protocol BinaryProtocol) fetchFromServer(index int, keys []string, withCAS bool) (map[string]*Item, error) {
	count := len(keys)
	buffer := new(bytes.Buffer)
	for index, key := range keys {
		op := operations["getkq"]
		keyLength := len(key)
		// the last item must be GetK to get a response with key
		if index == count-1 {
			op = operations["getk"]
		}
		pkt := &packet{
			header: header{
				magic:      requestMagic,
				opcode:     op.opcode,
				keyLength:  uint16(keyLength),
				bodyLength: uint32(keyLength),
			}, key: key}
		if err := pkt.write(buffer); err != nil {
			return nil, err
		}
	}
	pool := protocol.pools[index]
	conn, err := pool.Get()
	if err != nil {
		return nil, err
	}
	if _, err = buffer.WriteTo(conn); err != nil {
		conn.SetError(err)
		pool.Put(conn)
		return nil, err
	}
	lastKey := keys[count-1]
	results := make(map[string]*Item, count)
	for {
		pkt := new(packet)
		err = pkt.read(conn)
		if err != nil && err != ErrItemNotFound {
			if pkt.status != 0 {
				conn.SetError(err)
			}
			pool.Put(conn)
			return nil, err
		}
		// skip if the key doesn't exist
		if err == ErrItemNotFound && pkt.key != lastKey {
			continue
		}
		if pkt.value != nil {
			var flags uint32
			if pkt.extras != nil {
				flags = binary.BigEndian.Uint32(pkt.extras)
			}
			results[pkt.key] = &Item{Key: pkt.key, Value: pkt.value, Flags: flags, CAS: pkt.cas}
		}
		if pkt.key == lastKey {
			break
		}
	}
	if err = pool.Put(conn); err != nil {
		return nil, err
	}
	return results, nil
}

// Store for store items to the server
func (protocol BinaryProtocol) store(cmd string, item *Item) error {
	if cmd == "cas" {
		cmd = "set"
	}
	op, ok := operations[cmd]
	if !ok {
		return ErrOperationNotSupported
	}
	keyLength := len(item.Key)
	pkt := &packet{
		header: header{
			magic:      requestMagic,
			opcode:     op.opcode,
			keyLength:  uint16(keyLength),
			cas:        item.CAS,
			bodyLength: uint32(keyLength),
		}, key: item.Key}
	if isStoreOperation(op) {
		pkt.value = item.Value
		extrasLength := 8
		pkt.extras = make([]byte, extrasLength)
		binary.BigEndian.PutUint32(pkt.extras[:4], item.Flags)
		binary.BigEndian.PutUint32(pkt.extras[4:], item.Expiration)
		pkt.extrasLength = uint8(extrasLength)
		pkt.bodyLength = uint32(pkt.keyLength) + uint32(len(pkt.value)) + uint32(extrasLength)
	}
	if op.command == "incr" || op.command == "decr" {
		delta, err := strconv.ParseUint(string(item.Value), 10, 64)
		if err != nil {
			return errors.New("invalid arguments")
		}
		extrasLength := 20
		pkt.extras = make([]byte, extrasLength)
		binary.BigEndian.PutUint64(pkt.extras[:8], delta)
		binary.BigEndian.PutUint64(pkt.extras[8:16], 0)
		binary.BigEndian.PutUint32(pkt.extras[16:], item.Expiration)
		pkt.extrasLength = uint8(extrasLength)
		pkt.bodyLength = uint32(pkt.keyLength) + uint32(extrasLength)
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
	if err = pkt.write(conn); err != nil {
		conn.SetError(err)
		pool.Put(conn)
		return err
	}
	if op.quiet {
		pool.Put(conn)
		return err
	}
	if err := pkt.read(conn); err != nil {
		if pkt.status != 0 {
			conn.SetError(err)
		}
		pool.Put(conn)
		return err
	}
	item.Value = pkt.value
	item.CAS = pkt.cas
	pool.Put(conn)
	return nil
}
