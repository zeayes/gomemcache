package gomemcache

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"
)

var client, textClient *Client

func init() {
	var err error
	client, err = NewClient("127.0.0.1:11211")
	if err != nil {
		os.Exit(1)
	}
	client.SetProtocol("binary")
	textClient, err = NewClient("127.0.0.1:11211")
	if err != nil {
		os.Exit(1)
	}
}

type TestCase struct {
	client   *Client
	protocol string
	command  string
}

func TestSet(t *testing.T) {
	testcases := []TestCase{
		{client: client, protocol: "binary", command: "set"},
		{client: textClient, protocol: "text", command: "set"},
	}
	for _, testcase := range testcases {
		key := fmt.Sprintf("test_%s_%s_key", testcase.protocol, testcase.command)
		value := []byte(fmt.Sprintf("test_%s_%s_value", testcase.protocol, testcase.command))
		if err := testcase.client.Set(&Item{Key: key, Value: value}); err != nil {
			t.Fatalf("client %s set error: %v", testcase.protocol, err)
		}
	}
}

func TestGet(t *testing.T) {
	testcases := []TestCase{
		{client: client, protocol: "binary", command: "get"},
		{client: textClient, protocol: "text", command: "get"},
	}
	for _, testcase := range testcases {
		flags := uint32(1000)
		expiration := 1
		key := fmt.Sprintf("test_%s_%s_key", testcase.protocol, testcase.command)
		value := []byte(fmt.Sprintf("test_%s_%s_value", testcase.protocol, testcase.command))
		item := &Item{Key: key, Value: value, Flags: flags, Expiration: uint32(expiration)}
		if err := testcase.client.Set(item); err != nil {
			t.Fatalf("client %s set error: %v", testcase.protocol, err)
		}
		result, err := testcase.client.Get(key)
		if err != nil {
			t.Fatalf("client %s get error: %v", testcase.protocol, err)
		}
		if result == nil {
			t.Fatalf("client %s TestGet value expect: %v but got nil", testcase.protocol, string(value))
		}
		if !bytes.Equal(value, result.Value) {
			t.Fatalf("client %s TestGet value expect: %v but got: %v", testcase.protocol, string(value), string(result.Value))
		}
		if flags != result.Flags {
			t.Fatalf("client %s TestGet flags expect: %v but got: %v", testcase.protocol, flags, result.Flags)
		}
		time.Sleep(time.Duration(expiration+1) * time.Second)
		result, err = testcase.client.Get(key)
		if err != nil {
			t.Fatalf("client %s get error: %v", testcase.protocol, err)
		}
		if result != nil {
			t.Fatalf("client %s get expired item got: %v", testcase.protocol, result)
		}
	}
}

func TestAdd(t *testing.T) {
	testcases := []TestCase{
		{client: client, protocol: "binary", command: "add"},
		{client: textClient, protocol: "text", command: "add"},
	}
	for _, testcase := range testcases {
		key := fmt.Sprintf("test_%s_%s_key", testcase.protocol, testcase.command)
		value := []byte(fmt.Sprintf("test_%s_%s_value", testcase.protocol, testcase.command))
		client.SetNoreply(false)
		if err := testcase.client.Delete(key); err != nil && err != ErrItemNotFound {
			t.Fatalf("test %s add delete error: %v", testcase.protocol, err)
		}
		if err := testcase.client.Add(&Item{Key: key, Value: value}); err != nil {
			t.Fatalf("test %s add error: %v", testcase.protocol, err)
		}
		err := testcase.client.Add(&Item{Key: key, Value: value})
		if err == nil {
			t.Fatalf("test %s add second execute should error", testcase.protocol)
		}
	}
}

func TestCAS(t *testing.T) {
	testcases := []TestCase{
		{client: client, protocol: "binary", command: "cas"},
		{client: textClient, protocol: "text", command: "cas"},
	}
	for _, testcase := range testcases {
		key := fmt.Sprintf("test_%s_%s_key", testcase.protocol, testcase.command)
		value := []byte(fmt.Sprintf("test_%s_%s_value", testcase.protocol, testcase.command))
		if err := testcase.client.Set(&Item{Key: key, Value: value}); err != nil {
			t.Fatalf("cas %s first set error: %v", testcase.protocol, err)
		}
		item, err := testcase.client.Get(key)
		if err != nil {
			t.Fatalf("cas %s get error: %v", testcase.protocol, err)
		}
		if err = testcase.client.Set(&Item{Key: key, Value: []byte("test_value_cas_2")}); err != nil {
			t.Fatalf("cas %s second set error: %v", testcase.protocol, err)
		}
		err = testcase.client.CAS(&Item{Key: key, Value: []byte("test_value_cas_3"), CAS: item.CAS})
		if err == nil {
			t.Fatalf("cas %s set expect fail, but successful", testcase.protocol)
		}
	}
}

func TestReplace(t *testing.T) {
	testcases := []TestCase{
		{client: client, protocol: "binary", command: "replace"},
		{client: textClient, protocol: "text", command: "replace"},
	}
	for _, testcase := range testcases {
		key := fmt.Sprintf("test_%s_%s_key", testcase.protocol, testcase.command)
		value := []byte(fmt.Sprintf("test_%s_%s_value", testcase.protocol, testcase.command))
		if err := testcase.client.Delete(key); err != nil && err != ErrItemNotFound {
			t.Fatalf("test add delete error: %v", err)
		}
		if err := testcase.client.Replace(&Item{Key: key, Value: value}); err == nil {
			t.Fatalf("test replace when key not exist must raise error")
		}
		if err := testcase.client.Set(&Item{Key: key, Value: []byte("test_replace_value")}); err != nil {
			t.Fatalf("test replace set error: %v", err)
		}
		if err := testcase.client.Replace(&Item{Key: key, Value: value}); err != nil {
			t.Fatalf("test replace error: %v", err)
		}
		item, err := testcase.client.Get(key)
		if err != nil {
			t.Fatalf("test replace get error: %v", err)
		}
		if !bytes.Equal(item.Value, value) {
			t.Fatalf("test replace expect %v got %v", item.Value, value)
		}
	}
}

func TestMultiGet(t *testing.T) {
	testcases := []TestCase{
		{client: client, protocol: "binary", command: "multiGet"},
		{client: textClient, protocol: "text", command: "multiGet"},
	}
	for _, testcase := range testcases {
		num := 10
		keys := make([]string, 0, num)
		expect := make(map[string][]byte, num)
		for i := 0; i < num; i++ {
			key := fmt.Sprintf("test_%s_%s_key_%d", testcase.protocol, testcase.command, i)
			value := []byte(fmt.Sprintf("test_%s_%s_value_%d", testcase.protocol, testcase.command, i))
			item := &Item{Key: key, Value: value}
			if err := testcase.client.Set(item); err != nil {
				t.Fatalf("set error: %v", err)
			}
			keys = append(keys, key)
			expect[key] = value
		}
		result, err := testcase.client.MultiGet(keys)
		if err != nil {
			t.Fatalf("get error: %v", err)
		}
		for k, v := range expect {
			if _, ok := result[k]; !ok {
				t.Fatalf("MultiGet key: %s not exsist", k)
			}
			if !bytes.Equal(v, result[k].Value) {
				t.Fatalf("MultiGet key: %s expect: %v got: %v", k, v, result[k])
			}
		}
	}
}

func TestMultiGetWithRepeatKeys(t *testing.T) {
	testcases := []TestCase{
		{client: client, protocol: "binary", command: "multiGet"},
		{client: textClient, protocol: "text", command: "multiGet"},
	}
	for _, testcase := range testcases {
		num := 10
		keys := make([]string, 0, num)
		expect := make(map[string][]byte, num)
		for i := 0; i < num; i++ {
			key := fmt.Sprintf("test_%s_%s_rkey_%d", testcase.protocol, testcase.command, i)
			value := []byte(fmt.Sprintf("test_%s_%s_rvalue_%d", testcase.protocol, testcase.command, i))
			item := &Item{Key: key, Value: value}
			if err := testcase.client.Set(item); err != nil {
				t.Fatalf("set error: %v", err)
			}
			keys = append(keys, key)
			if i%3 == 0 {
				keys = append(keys, key)
			}
			expect[key] = value
		}
		result, err := testcase.client.MultiGet(keys)
		if err != nil {
			t.Fatalf("get error: %v", err)
		}
		for k, v := range expect {
			if _, ok := result[k]; !ok {
				t.Fatalf("MultiGet %s key: %s not exsist", testcase.protocol, k)
			}
			if !bytes.Equal(v, result[k].Value) {
				t.Fatalf("MultiGet %s key: %s expect: %v got: %v", testcase.protocol, k, v, result[k])
			}
		}
	}
}

func TestDelete(t *testing.T) {
	testcases := []TestCase{
		{client: client, protocol: "binary", command: "delete"},
		{client: textClient, protocol: "text", command: "delete"},
	}
	for i, testcase := range testcases {
		key := fmt.Sprintf("test_%s_%s_key_%d", testcase.protocol, testcase.command, i)
		value := []byte(fmt.Sprintf("test_%s_%s_value_%d", testcase.protocol, testcase.command, i))
		item := &Item{Key: key, Value: value}
		if err := testcase.client.Set(item); err != nil {
			t.Fatalf("set error: %v", err)
		}
		if err := testcase.client.Delete(key); err != nil {
			t.Fatalf("delete error: %v", err)
		}
		result, err := testcase.client.Get(key)
		if err != nil {
			t.Fatalf("get error: %v", err)
		}
		if result != nil {
			t.Fatalf("TestDelete expect result: nil but got: %v", result)
		}
	}
}

func BenchmarkBinarySet(b *testing.B) {
	item := &Item{Key: "bench_binary_set", Value: []byte("world")}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := client.Set(item); err != nil {
			b.Fatalf("set error: %v", err)
		}
	}
}

func BenchmarkBinaryGet(b *testing.B) {
	key := "bench_binary_get"
	value := []byte("world")
	item := &Item{Key: key, Value: value}
	if err := client.Set(item); err != nil {
		b.Fatalf("set error: %v", err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		result, err := client.Get(key)
		if err != nil {
			b.Fatalf("get error: %v", err)
		}
		if !bytes.Equal(value, result.Value) {
			b.Fatalf("TestGet expect: %v but got: %v", value, result)
		}
	}
}

func BenchmarkBinaryMultiGet(b *testing.B) {
	value := []byte("world")
	keys := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("bench_binary_%d", i)
		item := &Item{Key: key, Value: value}
		if err := client.Set(item); err != nil {
			b.Fatalf("set error: %v", err)
		}
		keys = append(keys, key)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := client.MultiGet(keys)
		if err != nil {
			b.Fatalf("get error: %v", err)
		}
	}
}

func BenchmarkTextSet(b *testing.B) {
	item := &Item{Key: "bench_text_set", Value: []byte("world")}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := textClient.Set(item); err != nil {
			b.Fatalf("set error: %v", err)
		}
	}
}

func BenchmarkTextGet(b *testing.B) {
	key := "bench_text_get"
	value := []byte("world")
	item := &Item{Key: key, Value: value}
	if err := textClient.Set(item); err != nil {
		b.Fatalf("set error: %v", err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		result, err := textClient.Get(key)
		if err != nil {
			b.Fatalf("get error: %v", err)
		}
		if !bytes.Equal(value, result.Value) {
			b.Fatalf("TestGet expect: %v but got: %v", value, result)
		}
	}
}

func BenchmarkTextMultiGet(b *testing.B) {
	value := []byte("world")
	keys := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("bench_text_%d", i)
		item := &Item{Key: key, Value: value}
		if err := textClient.Set(item); err != nil {
			b.Fatalf("set error: %v", err)
		}
		keys = append(keys, key)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := textClient.MultiGet(keys)
		if err != nil {
			b.Fatalf("get error: %v", err)
		}
	}
}
