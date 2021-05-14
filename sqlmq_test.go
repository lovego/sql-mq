package sqlmq

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
)

func ExampleSqlMQ() {
	db := getDB()
	if _, err := db.Exec("DROP TABLE IF EXISTS sqlmq"); err != nil {
		panic(err)
	}
	mq := &SqlMQ{
		DB:    db,
		Table: NewStdTable(db, "sqlmq"),
	}

	mq.Register("test", func(ctx context.Context, tx *sql.Tx, msg Message) (time.Duration, error) {
		m := msg.(*StdMessage)
		var data string
		if err := json.Unmarshal(m.Data.([]byte), &data); err != nil {
			return 0, err
		}
		m.Data = data
		fmt.Println(data, m.TryCount)
		switch m.Data {
		case "success":
			return 0, nil
		case "retry":
			if m.TryCount < 2 {
				return time.Second, errors.New("error happened")
			} else {
				return 0, nil
			}
		default:
			return -1, errors.New("given up")
		}
	})
	produce(mq, "success")
	produce(mq, "retry")
	produce(mq, "given up")

	go mq.Consume()
	time.Sleep(3 * time.Second)
	// Output:
	// success 0
	// retry 0
	// given up 0
	// retry 1
	// retry 2
}

func getDB() *sql.DB {
	db, err := sql.Open("postgres", "postgres://postgres:postgres@localhost/postgres?sslmode=disable")
	if err != nil {
		log.Panic(err)
	}
	return db
}

func produce(mq *SqlMQ, data string) {
	if tx, err := mq.DB.BeginTx(context.Background(), nil); err != nil {
		panic(err)
	} else {
		if err := mq.Produce(tx, &StdMessage{Queue: "test", Data: data}); err != nil {
			panic(err)
		}
		tx.Commit()
	}
}
