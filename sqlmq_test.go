// some cases are just for a better coverage, useless.
package sqlmq

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/lovego/logger"
)

func ExampleSqlMQ_Register() {
	fmt.Println(testMQ.Register("test3", noopHandler))
	fmt.Println(testMQ.Register("test3", noopHandler))
	// Output:
	// <nil>
	// queue test3 already registered
}

func ExampleSqlMQ_Produce() {
	fmt.Println(testMQ.Produce(nil, &StdMessage{Queue: "test2"}))

	if err := testMQ.Register("test2", noopHandler); err != nil {
		panic(err)
	}

	tx, err := testDB.Begin()
	if err != nil {
		panic(err)
	}
	err = testMQ.Produce(tx, &StdMessage{Queue: "test2"})
	if err != nil {
		tx.Rollback()
	} else {
		err = tx.Commit()
	}
	fmt.Println(err)

	fmt.Println(testMQ.Produce(tx, &StdMessage{Queue: "test2", Data: make(chan int)}))

	// Output:
	// unknown queue: test2
	// <nil>
	// json: unsupported type: chan int
}

func noopHandler(ctx context.Context, tx *sql.Tx, msg Message) (time.Duration, bool, error) {
	return 0, true, nil
}

func ExampleSqlMQ_validate() {
	var mq SqlMQ
	fmt.Println(mq.validate())
	mq.DB = testMQ.DB
	fmt.Println(mq.validate())
	mq.Table = testMQ.Table
	fmt.Println(mq.validate())
	// Output:
	// SqlMQ.DB must not be nil
	// SqlMQ.Table must not be nil
	// <nil>
}

func ExampleSqlMQ_Consume_panic() {
	defer func() {
		fmt.Println(strings.HasSuffix(recover().(string), "SqlMQ.DB must not be nil"))
	}()
	var mq SqlMQ
	mq.Consume()
	// Output:
	// true
}

func ExampleSqlMQ_Consume() {
	var mq = getSqlMQ()
	go mq.Consume()
	time.Sleep(10 * time.Millisecond)
	// Output:
}

func ExampleSqlMQ_consume() {
	var mq = recreateSqlMQ()
	if err := mq.Register("test3", noopHandler); err != nil {
		panic(err)
	}
	mq.TxTimeout = time.Nanosecond // set smallest time to make it timeout
	fmt.Println(mq.consume(2*time.Minute, 3*time.Minute))
	mq.TxTimeout = 0

	fmt.Println(mq.consume(2*time.Minute, 3*time.Minute))

	mq.Produce(nil, &StdMessage{Queue: "test3", RetryAt: time.Now().Add(time.Hour)})
	fmt.Println(mq.consume(2*time.Minute, 3*time.Minute))

	// Output:
	// 3m0s
	// 2m0s
	// 2m0s
}

func ExampleSqlMQ_handle() {
	var mq = getSqlMQ()
	tx, cancel, err := mq.beginTx()
	if err != nil {
		panic(err)
	}
	fmt.Println(mq.handle(context.Background(), cancel, tx, &StdMessage{Queue: "test"}))

	// Output:
	// 1m0s unknown queue: test
}

func ExampleSqlMQ_markFail() {
	var mq = getSqlMQ()
	var buf bytes.Buffer
	mq.Logger = logger.New(&buf)
	mq.markFail(mq.DB, &StdMessage{}, -1, false)
	fmt.Println(bytes.Contains(buf.Bytes(), []byte(`"msg":"affected 0 rows"`)))
	// Output:
	// true
}
