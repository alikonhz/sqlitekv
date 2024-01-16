package kvstore

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func BenchmarkSet(b *testing.B) {
	fileName := "TestSetGet.sqlite"
	assert.NoError(b, removeFiles(fileName))
	s, err := NewKvStore(fileName)
	if err != nil {
		panic(err)
	}
	defer s.Close()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		val := KvString(fmt.Sprintf("value-%d", i))

		err = s.Set(context.Background(), key, val)
	}
}

func TestConcurrentSet(t *testing.T) {
	// read/write operations on the same key are not performed in parallel
	// they are queued and processed in order of arrival
	// we test this by spawning #count goroutines and then trying to read the value
	// after #count / 2 entries were processed we signal the test to continue and issue a read operation
	fileName := "TestConcurrentSet"
	s := mustCreateKvStore(t, fileName)
	defer s.Close()

	const (
		key   = "key"
		count = 10
	)

	// ignore the error
	s.Set(context.Background(), key, KvString("value"))

	values := generateValues(count)

	halfChan := make(chan struct{})
	writeChan := make(chan int, count)

	defer close(halfChan)

	go func() {
		var wg sync.WaitGroup
		wg.Add(count)
		for i := 0; i < count; i++ {
			i := i
			go func() {
				err := s.Set(context.Background(), key, KvString(values[i]))
				assert.NoError(t, err)
				writeChan <- i

				if i == count/2 {
					halfChan <- struct{}{}
				}

				wg.Done()
			}()
		}

		wg.Wait()
		close(writeChan)
	}()

	doneChan := make(chan struct{})
	defer close(doneChan)

	var readVal *KvValue
	go func() {
		// wait until half of the workers sent Set requests
		<-halfChan

		// try to read the value
		// if everything is correct then the Get request will be queued
		// and should return the latest value
		val, err := s.Get(context.Background(), key)
		assert.NoError(t, err)
		readVal = val
		doneChan <- struct{}{}
	}()

	<-doneChan

	// drain the channel
	var lastWritten int
	for i := range writeChan {
		lastWritten = i
	}

	extracted, _ := ConvertFromValue(readVal)
	assert.Equal(t, fmt.Sprintf("value-%d", lastWritten), extracted)
}

func generateValues(count int) []string {
	values := make([]string, count)
	for i := 0; i < count; i++ {
		values[i] = fmt.Sprintf("value-%d", i)
	}

	return values
}

func TestKeyNotFound(t *testing.T) {
	fileName := "TestKeyNotFound"
	s := mustCreateKvStore(t, fileName)
	defer s.Close()

	val, err := s.Get(context.Background(), "key")
	assert.Nil(t, val)
	assert.Error(t, ErrKeyNotFound, err)
}

func TestSetSameKey(t *testing.T) {
	fileName := "TestSetSameKey.sqlite"
	s := mustCreateKvStore(t, fileName)
	defer s.Close()

	const key = "key"
	value := []byte{1, 2, 3}
	err := s.Set(context.Background(), key, KvBinary(value))
	assert.NoError(t, err)

	newValue := []byte{1, 2, 3, 4}
	err = s.Set(context.Background(), key, KvBinary(newValue))
	assert.NoError(t, err)

	got, err := s.Get(context.Background(), key)
	assert.NoError(t, err)
	assert.Equal(t, newValue, got.Value)
	assert.Equal(t, BinaryType, got.Type)
}

func TestSetGet(t *testing.T) {
	fileName := "TestSetGet.sqlite"
	s := mustCreateKvStore(t, fileName)
	defer s.Close()

	runTypeTest[int32](t, "int32 type", s, "keyint32", 123)
	runTypeTest[int64](t, "int64 type", s, "keyint64", 981238917398)
	runTypeTest[float32](t, "float32 type", s, "keyfloat32", 7162.12398)
	runTypeTest[float64](t, "float64 type", s, "keyfloat64", 716219878712341249.123981092381)
	runTypeTest[string](t, "string type", s, "keystring", "this is string")
	runTypeTest[[]byte](t, "[]byte type", s, "keybyte", []byte{1, 2, 3, 4, 5})

	t.Run("int type", func(t *testing.T) {
		var i int = 182878

		key := "keyint"
		kv, err := ConvertToValue(i)
		assert.NoError(t, err)

		err = s.Set(context.Background(), key, kv)
		assert.NoError(t, err)

		got, err := s.Get(context.Background(), key)
		assert.NoError(t, err)

		v, err := ConvertFromValue(got)
		assert.NoError(t, err)

		// int will be stored as int64
		assert.Equal(t, int64(i), v)
	})
}

func runTypeTest[T any](t *testing.T, testName string, s *SqliteStorage, key string, val T) {
	t.Run(testName, func(t *testing.T) {
		kv, err := ConvertToValue(val)
		assert.NoError(t, err)

		err = s.Set(context.Background(), key, kv)
		assert.NoError(t, err)

		got, err := s.Get(context.Background(), key)
		assert.NoError(t, err)

		v, err := ConvertFromValue(got)
		assert.NoError(t, err)

		assert.Equal(t, val, v)
	})
}

func mustCreateKvStore(t *testing.T, fileName string) *SqliteStorage {
	assert.NoError(t, removeFiles(fileName))
	t.Cleanup(func() {
		removeFiles(fileName)
	})
	s, err := NewKvStore(fileName)
	if err != nil {
		panic(err)
	}

	return s
}

func removeFiles(name string) error {
	files, err := filepath.Glob(fmt.Sprintf("%s*", name))
	if err != nil {
		return err
	}

	for _, file := range files {
		err = os.Remove(file)
		if err != nil {
			return err
		}
	}

	return nil
}
