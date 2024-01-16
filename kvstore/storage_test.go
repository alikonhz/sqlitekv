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
		val := []byte(fmt.Sprintf("value-%d", i))

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
	s.Set(context.Background(), key, []byte("value"))

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
				err := s.Set(context.Background(), key, values[i])
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

	var readVal string
	go func() {
		// wait until half of the workers sent Set requests
		<-halfChan

		// try to read the value
		// if everything is correct then the Get request will be queued
		// and should return the latest value
		val, err := s.Get(context.Background(), key)
		assert.NoError(t, err)
		readVal = string(val)
		doneChan <- struct{}{}
	}()

	<-doneChan

	// drain the channel
	var lastWritten int
	for i := range writeChan {
		lastWritten = i
	}

	assert.Equal(t, fmt.Sprintf("value-%d", lastWritten), readVal)
}

func generateValues(count int) [][]byte {
	values := make([][]byte, count)
	for i := 0; i < count; i++ {
		val := []byte(fmt.Sprintf("value-%d", i))
		values[i] = val
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
	err := s.Set(context.Background(), key, value)
	assert.NoError(t, err)

	newValue := []byte{1, 2, 3, 4}
	err = s.Set(context.Background(), key, newValue)
	assert.NoError(t, err)

	got, err := s.Get(context.Background(), key)
	assert.NoError(t, err)
	assert.Equal(t, newValue, got)
}

func TestSetGet(t *testing.T) {
	fileName := "TestSetGet.sqlite"
	s := mustCreateKvStore(t, fileName)
	defer s.Close()

	const key = "key"
	value := []byte{1, 2, 3, 4}

	err := s.Set(context.Background(), key, value)
	assert.NoError(t, err)

	got, err := s.Get(context.Background(), key)
	assert.NoError(t, err)

	assert.Equal(t, value, got)
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
