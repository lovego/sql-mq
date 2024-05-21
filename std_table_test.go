// some cases are just for a better coverage, useless.
package sqlmq

import (
	"context"
	"fmt"
	"time"
)

func ExampleNewStdTable() {
	table := NewStdTable(testDB, "test_name", -1)
	fmt.Println(table.Name(), table.keep)
	// Output:
	// test_name 24h0m0s
}

func ExampleCreateTable() {
	defer func() {
		fmt.Println(recover() != nil)
	}()
	createTable(testDB, "create table")
	// Output:
	// true
}

func ExampleCreateIndex() {
	defer func() {
		fmt.Println(recover() != nil)
	}()
	createIndex(testDB, "create index")
	// Output:
	// true
}

func ExampleTimeoutTx() {
	table := NewStdTable(testDB, "test_table", 0)
	ctx, _ := context.WithTimeout(context.Background(), time.Millisecond)
	tx, err := testDB.BeginTx(ctx, nil)
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Millisecond)

	_, err = table.EarliestMessage(tx)
	fmt.Println(err != nil)

	fmt.Println(table.MarkSuccess(tx, &StdMessage{}) != nil)
	fmt.Println(table.MarkRetry(tx, &StdMessage{}, 0) != nil)
	fmt.Println(table.MarkGivenUp(tx, &StdMessage{}) != nil)

	fmt.Println(table.ProduceMessage(tx, &StdMessage{}) != nil)

	// Output:
	// true
	// true
	// true
	// true
	// true
}

func ExampleClosedDB() {
	table := NewStdTable(testDB, "test_table", 0)
	var db = getDB()
	db.Close()

	_, err := table.CleanMessages(db)
	fmt.Println(err != nil)
	// Output:
	// true
}
