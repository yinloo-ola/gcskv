package gcskv

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"testing"
	"time"
)

var size = 200

func init() {
	seed := time.Now().UnixNano()
	fmt.Println(seed)
	rand.Seed(seed)
}
func TestGcsStore(t *testing.T) {
	kvs_set := genKeyValues(size)
	kvs_get := genKeyValues(size)
	kvs_del := genKeyValues(size)

	gcsStore, err := New("mrc_research", "gcskv/")
	if err != nil {
		t.Fatalf("fail to create GCS Store. %v", err)
	}
	for _, kv := range kvs_set {
		err := gcsStore.Set(kv.Key, kv.Value)
		if err != nil {
			t.Fatalf("Set failed. %v", err)
		}
	}

	for _, kv := range kvs_get {
		out, err := gcsStore.Get(kv.Key)
		if err != nil {
			t.Fatalf("Get failed. %v", err)
		}
		expected := expectedOutput(kv.Key)
		if !bytes.Equal(out, expected) {
			t.Fatalf("Gotten %s for key %s. Expected %s", string(out), kv.Key, string(expected))
		}
	}

	countLeft := 0
	for i, kv := range kvs_del {
		if i%2 == 0 {
			countLeft++
			continue
		}
		err := gcsStore.Del(kv.Key)
		if err != nil {
			t.Fatalf("Del failed. %v", err)
		}
	}
	size, err := gcsStore.Size()
	if err != nil {
		t.Fatalf("Get Size failed. %v", err)
	}
	if size != countLeft {
		t.Fatalf("store size %d is not equal to count left after deleting %d", size, countLeft)
	}

	for i, kv := range kvs_del {
		if i%2 == 0 {
			out, err := gcsStore.Get(kv.Key)
			if err != nil {
				t.Fatalf("Get failed. %v", err)
			}
			expected := expectedOutput(kv.Key)
			if !bytes.Equal(out, expected) {
				t.Fatalf("Gotten %s for key %s. Expected %s", string(out), kv.Key, string(expected))
			}
			continue
		}
		out, err := gcsStore.Get(kv.Key)
		if err == nil {
			t.Fatalf("Get %s should fail but still returns %s", kv.Key, string(out))
		}
	}
	err = gcsStore.Clear()
	if err != nil {
		t.Fatalf("Clear failed. %v", err)
	}
	if size, errSize := gcsStore.Size(); size != 0 {
		t.Fatalf("%d objects left in store after Clear()", size)
	} else if errSize != nil {
		t.Fatalf("Size() failed")
	}
}

func TestScan(t *testing.T) {
	kvs := genKeyValues(200, "folder1/")
	gcsStore, err := New("mrc_research", "gcskv/")
	if err != nil {
		t.Fatalf("fail to create GCS Store. %v", err)
	}
	for _, kv := range kvs {
		err := gcsStore.Set(kv.Key, kv.Value)
		if err != nil {
			t.Fatalf("Set failed. %v", err)
		}
	}

	expectedKeys := make([]string, 0, 200)
	for _, kv := range kvs {
		if kv.Key >= "folder1/11" && kv.Key < "folder1/20" {
			expectedKeys = append(expectedKeys, kv.Key)
		}
	}
	sort.Strings(expectedKeys)

	keys, err := gcsStore.Scan("folder1/", "11", "20")
	if err != nil {
		t.Fatalf("Scan failed. %v", err)
	}
	if len(keys) != len(expectedKeys) {
		t.Fatalf("expected 11 keys, got %d", len(keys))
	}
	for i, k := range keys {
		if k != expectedKeys[i] {
			t.Fatalf("unexpected key present: %s", k)
		}
	}

	gcsStore.Clear()
}

func contains(key string, list []string) bool {
	for _, k := range list {
		if k == key {
			return true
		}
	}
	return false
}

func expectedOutput(key string) []byte {
	keyInt, _ := strconv.Atoi(key)
	return []byte(strconv.Itoa(size - keyInt))
}

func BenchmarkSet(b *testing.B) {
	kvs := genKeyValues(size)
	store, _ := New("mrc_research", "gcskv")
	b.ResetTimer()
	i := 0
outer:
	for i < b.N {
		for _, kv := range kvs {
			store.Set(kv.Key, kv.Value)
			i++
			if i >= b.N {
				break outer
			}
		}
	}

	b.StopTimer()
	store.Clear()
}

func BenchmarkGet(b *testing.B) {
	kvs := genKeyValues(size)
	store, _ := New("mrc_research", "gcskv")
	for _, kv := range kvs {
		store.Set(kv.Key, kv.Value)
	}
	kvs = genKeyValues(size)
	b.ResetTimer()
	i := 0
outer:
	for i < b.N {
		for _, kv := range kvs {
			store.Get(kv.Key)
			i++
			if i >= b.N {
				break outer
			}
		}
	}
	b.StopTimer()
	store.Clear()
}

func BenchmarkDel(b *testing.B) {
	kvs := genKeyValues(size)
	store, _ := New("mrc_research", "gcskv")
	for _, kv := range kvs {
		store.Set(kv.Key, kv.Value)
	}
	kvs = genKeyValues(size)
	b.ResetTimer()
	i := 0
outer:
	for i < b.N {
		for _, kv := range kvs {
			store.Del(kv.Key)
			i++
			if i >= b.N {
				break outer
			}
		}
	}
	b.StopTimer()
	store.Clear()
}

func BenchmarkScan(b *testing.B) {
	kvs := genKeyValues(200)
	store, _ := New("mrc_research", "gcskv")
	for _, kv := range kvs {
		store.Set(kv.Key, kv.Value)
	}

	b.ResetTimer()
	var keys []string
	for i := 0; i < b.N; i++ {
		keys, _ = store.Scan("folder1/", "11", "20")
	}

	b.StopTimer()
	println(len(keys))
	store.Clear()
}

func BenchmarkClear(b *testing.B) {
	store, _ := New("mrc_research", "gcskv")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		kvs := genKeyValues(size)
		for _, kv := range kvs {
			store.Set(kv.Key, kv.Value)
		}
		b.StartTimer()
		store.Clear()
	}

}

type KeyValue struct {
	Key   string
	Value []byte
}

func genKeyValues(count int, prefixes ...string) []KeyValue {
	kvs := make([]KeyValue, 0, count)

	fullPrefix := ""
	for _, prefix := range prefixes {
		fullPrefix += prefix
	}
	for _, v := range rand.Perm(count) {
		k := fullPrefix + strconv.Itoa(v)
		v := strconv.Itoa(count - v)
		kv := KeyValue{Key: k, Value: []byte(v)}
		kvs = append(kvs, kv)
	}
	return kvs
}
