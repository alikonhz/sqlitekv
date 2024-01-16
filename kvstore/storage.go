package kvstore

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math/big"
	"math/rand"
	"sync/atomic"
	"time"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

var (
	_ = KvStore((*SqliteStorage)(nil))
)

var (
	ErrConnNotOpened = errors.New("unable to open connection to the database")
	ErrKeyNotFound   = errors.New("key not found")
	ErrStopped       = errors.New("server has been stopped")
)

const (
	createTable = "CREATE TABLE IF NOT EXISTS kvstore (key TEXT PRIMARY KEY, value BLOB NULL)"
	insertSql   = "INSERT INTO kvstore (key, value) VALUES (?,?) ON CONFLICT (key) DO UPDATE SET value = excluded.value"
	selectSql   = "SELECT value FROM kvstore WHERE key = ?"
)

type KvStore interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte) error
}

type SqliteStorage struct {
	dbPool *sqlitex.Pool

	workerPool []*worker
}

func NewKvStore(fileName string) (*SqliteStorage, error) {
	dbPool, err := sqlitex.Open(fmt.Sprintf("%s", fileName), sqlite.OpenWAL|sqlite.OpenReadWrite|sqlite.OpenCreate, 10)
	if err != nil {
		return nil, err
	}

	s := &SqliteStorage{
		dbPool: dbPool,
	}

	err = s.create()
	if err != nil {
		return nil, err
	}

	const poolSize = 8
	s.workerPool = make([]*worker, poolSize) // default worker size is 8
	for i := 0; i < poolSize; i++ {
		c := make(chan *workerRequest)
		var ac atomic.Pointer[chan *workerRequest]
		ac.Store(&c)

		w := &worker{
			dbPool: s.dbPool,
			c:      ac,
		}
		s.workerPool[i] = w

		go w.process()
	}

	return s, nil
}

func (s *SqliteStorage) Close() error {
	for i := 0; i < len(s.workerPool); i++ {
		s.workerPool[i].stop()
	}

	return s.dbPool.Close()
}

func (s *SqliteStorage) create() error {
	return withConn(context.Background(), s.dbPool, func(conn *sqlite.Conn) error {
		return runStmt(conn, createTable)
	})
}

func runStmt(conn *sqlite.Conn, query string) error {
	stmt, err := conn.Prepare(query)
	if err != nil {
		return err
	}

	_, err = stmt.Step()
	if err != nil {
		return err
	}
	err = stmt.Finalize()
	if err != nil {
		return err
	}

	return nil
}

func (s *SqliteStorage) Get(ctx context.Context, key string) ([]byte, error) {
	respChan := make(chan workerResponse)
	r := &workerRequest{
		ctx:      ctx,
		op:       GetOp,
		key:      key,
		respChan: respChan,
	}

	w := s.getWorker(key)
	err := w.send(r)
	if err != nil {
		return nil, err
	}

	return s.waitForResp(r)
}

func (s *SqliteStorage) Set(ctx context.Context, key string, value []byte) error {
	respChan := make(chan workerResponse)
	r := &workerRequest{
		ctx:      ctx,
		key:      key,
		op:       SetOp,
		value:    value,
		respChan: respChan,
	}
	w := s.getWorker(key)
	err := w.send(r)
	if err != nil {
		return err
	}

	_, err = s.waitForResp(r)

	return err
}

func (s *SqliteStorage) getWorker(key string) *worker {
	b := big.NewInt(0)
	hash := md5.New()
	hash.Write([]byte(key))
	hexStr := hex.EncodeToString(hash.Sum(nil))
	b.SetString(hexStr, 16)

	val := b.Int64()
	wIndex := val % int64(len(s.workerPool))
	if wIndex < 0 {
		wIndex *= -1
	}

	return s.workerPool[wIndex]
}

func (s *SqliteStorage) waitForResp(r *workerRequest) ([]byte, error) {
	defer close(r.respChan)

	select {
	case <-r.ctx.Done():
		return nil, r.ctx.Err()
	case resp := <-r.respChan:
		return resp.value, resp.err
	}
}

func withConnRes[T any](ctx context.Context, dbPool *sqlitex.Pool, f func(conn *sqlite.Conn) (T, error), maxRetries int) (T, error) {
	conn := dbPool.Get(ctx)
	if conn == nil {
		var v T
		return v, ErrConnNotOpened
	}

	defer dbPool.Put(conn)

	return f(conn)
}

func withConnRetry(ctx context.Context, dbPool *sqlitex.Pool, f func(conn *sqlite.Conn) error, maxRetries int) error {
	var conn *sqlite.Conn
	for i := 0; i < maxRetries && conn == nil; i++ {
		const maxMillis = 500
		m := time.Duration(rand.Int31n(maxMillis)) * time.Millisecond
		time.Sleep(m)
		conn = dbPool.Get(ctx)
	}

	if conn == nil {
		return ErrConnNotOpened
	}

	defer dbPool.Put(conn)

	return f(conn)
}

func withConn(ctx context.Context, dbPool *sqlitex.Pool, f func(conn *sqlite.Conn) error) error {
	conn := dbPool.Get(ctx)
	if conn == nil {
		return ErrConnNotOpened
	}

	defer dbPool.Put(conn)

	return f(conn)
}

type opCode byte

const (
	GetOp opCode = 0
	SetOp opCode = 1
	DelOp opCode = 2
)

type worker struct {
	dbPool *sqlitex.Pool
	c      atomic.Pointer[chan *workerRequest]
}

type workerResponse struct {
	value []byte
	err   error
}

type workerRequest struct {
	ctx      context.Context
	key      string
	value    []byte
	op       opCode
	respChan chan workerResponse
}

func (w *worker) send(r *workerRequest) error {
	c := w.c.Load()
	if c == nil {
		return ErrStopped
	}

	*c <- r

	return nil
}

func (w *worker) process() {
	c := w.c.Load()
	for r := range *c {
		switch r.op {
		case GetOp:
			w.get(r.ctx, r.key, r.respChan)
		case SetOp:
			w.set(r.ctx, r.key, r.value, r.respChan)
		}
	}
}

func (w *worker) set(ctx context.Context, key string, value []byte, respChan chan workerResponse) {
	err := withConnRetry(ctx, w.dbPool, func(conn *sqlite.Conn) error {
		stmt, err := conn.Prepare(insertSql)
		if err != nil {
			return err
		}

		defer stmt.Finalize()
		stmt.BindText(1, key)
		if value != nil {
			stmt.BindBytes(2, value)
		} else {
			stmt.BindNull(2)
		}

		_, err = stmt.Step()
		return err
	}, 10)

	reply(respChan, SetOp, key, err)
}

func reply(respChan chan workerResponse, op opCode, key string, err error) {
	defer func() {
		// it might be that respChan will be closed
		// in this case send on closed channel will panic
		// so we just log an error and don't bother replying
		rErr := recover()
		if rErr != nil {
			log.Printf("error occurred while replying. op %d, key %s, err: %v\n", op, key, err)
		}
	}()

	if err != nil {
		respChan <- workerResponse{
			err: err,
		}

		return
	}

	respChan <- workerResponse{
		value: nil,
		err:   nil,
	}
}

func (w *worker) get(ctx context.Context, key string, respChan chan workerResponse) {
	res, err := withConnRes(ctx, w.dbPool, func(conn *sqlite.Conn) ([]byte, error) {
		stmt, err := conn.Prepare(selectSql)
		if err != nil {
			return nil, err
		}

		defer stmt.Finalize()
		stmt.BindText(1, key)
		hasRows, err := stmt.Step()
		if err != nil {
			return nil, err
		}

		if !hasRows {
			return nil, fmt.Errorf("%w: %s", ErrKeyNotFound, key)
		}

		reader := stmt.GetReader("value")
		res := make([]byte, reader.Len())
		_, err = reader.Read(res)
		if err != nil {
			return nil, err
		}

		return res, nil
	}, 10)

	if err != nil {
		respChan <- workerResponse{
			err: err,
		}

		return
	}

	respChan <- workerResponse{
		value: res,
		err:   nil,
	}
}

func (w *worker) stop() {
	c := w.c.Load()
	if c != nil {
		w.c.Store(nil)
		close(*c)
	}
}
