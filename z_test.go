package sqlmq

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	_ "github.com/lib/pq"
	"github.com/lovego/logger"
)

var testDB = getDB()
var testMQ = recreateSqlMQ()

func ExampleSqlMQ() {
	testMQ.Debug(true)
	if err := testMQ.Register("test", testHandler); err != nil {
		panic(err)
	}

	produce(testMQ, "success")
	produce(testMQ, "retry")
	produce(testMQ, "given up")

	go testMQ.Consume()
	time.Sleep(5 * time.Second)
	// Output:
	// success 0
	// retry 0
	// given up 0
	// retry 1
	// retry 2
}

func testHandler(ctx context.Context, tx *sql.Tx, msg Message) (time.Duration, bool, error) {
	m := msg.(*StdMessage)
	var data string
	if err := json.Unmarshal(m.Data.([]byte), &data); err != nil {
		return 0, true, err
	}
	m.Data = data
	fmt.Println(data, m.TriedCount)
	switch m.Data {
	case "success":
		return 0, true, nil
	case "retry":
		switch m.TriedCount {
		case 0:
			return time.Second, false, errors.New("error happened")
		case 1:
			return time.Second, true, errors.New("error happened")
		default:
			return 0, true, nil
		}
	default:
		return -1, false, errors.New("given up")
	}
}

func getDB() *sql.DB {
	db, err := sql.Open("postgres", "postgres://develop:@localhost/postgres?sslmode=disable")
	if err != nil {
		panic(err)
	}
	return db
}

func recreateSqlMQ() *SqlMQ {
	if _, err := testDB.Exec("DROP TABLE IF EXISTS sqlmq"); err != nil {
		panic(err)
	}
	return getSqlMQ()
}

func getSqlMQ() *SqlMQ {
	logFile, err := os.Create("log.json")
	if err != nil {
		panic(err)
	}

	return &SqlMQ{
		DB:            testDB,
		Table:         NewStdTable(testDB, "sqlmq", time.Hour),
		Logger:        logger.New(logFile),
		CleanInterval: time.Hour,
	}
}

func produce(mq *SqlMQ, data string) {
	if err := mq.Produce(nil, &StdMessage{Queue: "test", Data: data}); err != nil {
		panic(err)
	}
}
