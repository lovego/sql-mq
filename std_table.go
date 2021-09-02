package sqlmq

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lovego/errs"
)

const (
	statusWaiting = "waiting"
	statusDone    = "done"
	statusGivenUp = "givenUp"

	rfc3339Micro = "2006-01-02T15:04:05.999999Z07:00"
)

type StdMessage struct {
	Id        int64
	Queue     string
	Data      interface{}
	Status    string
	CreatedAt time.Time
	TryCount  uint16
	RetryAt   time.Time
}

func (msg *StdMessage) QueueName() string {
	return msg.Queue
}

func (msg *StdMessage) ConsumeAt() time.Time {
	return msg.RetryAt
}

// NewStdTable create a standard `sqlmq.Table` instance.
// db: db use to create table and index.
// name: database table name.
// keep: keep a successfully consumed message for how long before delete it.
func NewStdTable(db *sql.DB, name string, keep time.Duration) *StdTable {
	createTable(db, name)
	createIndex(db, name)
	if keep < 0 {
		keep = 24 * time.Hour
	}
	return &StdTable{name: name, keep: keep}
}

func createTable(db *sql.DB, name string) {
	var createSql = fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	id            bigserial    NOT NULL PRIMARY KEY,
	queue         text         NOT NULL,
	status        text         NOT NULL,
	created_at    timestamptz  NOT NULL,
	try_count     smallint     NOT NULL,
	retry_at      timestamptz  NOT NULL,
	data          jsonb        NOT NULL
);
`, name,
	)
	ctx, cancel := sqlTimeout()
	defer cancel()
	if _, err := db.ExecContext(ctx, createSql); err != nil {
		panic(time.Now().Format(time.RFC3339Nano) + " " + err.Error())
	}
}

func createIndex(db *sql.DB, name string) {
	var createSql = fmt.Sprintf(
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS %s_retry_at ON %s (retry_at)`, strings.Replace(name, ".", "_", 1), name,
	)
	ctx, cancel := sqlTimeout()
	defer cancel()
	if _, err := db.ExecContext(ctx, createSql); err != nil {
		panic(time.Now().Format(time.RFC3339Nano) + " " + err.Error())
	}
}

// StdTable is a standard `sqlmq.Table` implementation.
type StdTable struct {
	name               string
	keep               time.Duration
	queues             []string
	earliestMessageSql string
	mutex              sync.RWMutex
}

func (table *StdTable) SetQueues(queues []string) {
	table.mutex.Lock()
	defer table.mutex.Unlock()
	table.queues = queues
	table.earliestMessageSql = ""
}

func (table *StdTable) EarliestMessage(tx *sql.Tx) (Message, error) {
	row := StdMessage{}
	querysql := table.getEarliestMessageSql()
	ctx, cancel := sqlTimeout()
	defer cancel()
	if err := tx.QueryRowContext(ctx, querysql).Scan(
		&row.Id, &row.Queue, &row.Data, &row.Status, &row.CreatedAt, &row.TryCount, &row.RetryAt,
	); err == sql.ErrNoRows {
		return nil, nil
	} else if err != nil {
		return nil, errs.Trace(err)
	}
	return &row, nil
}

func (table *StdTable) getEarliestMessageSql() string {
	table.mutex.RLock()
	if table.earliestMessageSql == "" {
		var queues []string
		for _, queue := range table.queues {
			queues = append(queues, quote(queue))
		}
		sort.Strings(queues)
		querySql := fmt.Sprintf(`
		SELECT id, queue, data, status, created_at, try_count, retry_at
		FROM %s
		WHERE queue IN (%s) AND status = '%s'
		ORDER BY retry_at
		LIMIT 1
		FOR UPDATE SKIP LOCKED
		`,
			table.name, strings.Join(queues, ","), statusWaiting,
		)
		table.mutex.RUnlock()

		table.mutex.Lock()
		table.earliestMessageSql = querySql
		table.mutex.Unlock()

		return querySql
	}
	defer table.mutex.RUnlock()
	return table.earliestMessageSql
}

func (table *StdTable) MarkSuccess(tx *sql.Tx, message Message) error {
	sql := fmt.Sprintf(`
	UPDATE %s
	SET status = '%s', try_count = try_count+1, retry_at = '%s'
	WHERE id = %d
	`,
		table.name,
		statusDone, time.Now().Format(rfc3339Micro),
		message.(*StdMessage).Id,
	)
	ctx, cancel := sqlTimeout()
	defer cancel()
	if result, err := tx.ExecContext(ctx, sql); err != nil {
		return errs.Trace(err)
	} else {
		return checkAffectedOne(result)
	}
}

func (table *StdTable) MarkRetry(db DBOrTx, message Message, retryAfter time.Duration) error {
	sql := fmt.Sprintf(`
	UPDATE %s
	SET try_count = try_count + 1,  retry_at = '%s'
	WHERE id = %d
	`,
		table.name,
		time.Now().Add(retryAfter).Format(rfc3339Micro),
		message.(*StdMessage).Id,
	)
	ctx, cancel := sqlTimeout()
	defer cancel()
	if result, err := db.ExecContext(ctx, sql); err != nil {
		return errs.Trace(err)
	} else {
		return checkAffectedOne(result)
	}
}

func (table *StdTable) MarkGivenUp(db DBOrTx, message Message) error {
	sql := fmt.Sprintf(`
	UPDATE %s
	SET status = '%s', try_count = try_count + 1, retry_at = '%s'
	WHERE id = %d
	`,
		table.name,
		statusGivenUp, time.Now().Format(rfc3339Micro),
		message.(*StdMessage).Id,
	)
	ctx, cancel := sqlTimeout()
	defer cancel()
	if result, err := db.ExecContext(ctx, sql); err != nil {
		return errs.Trace(err)
	} else {
		return checkAffectedOne(result)
	}
}

// if ProduceMessage runs succussfully, message id is set in message(which is *StdMessage).
func (table *StdTable) ProduceMessage(db DBOrTx, message Message) error {
	msg := message.(*StdMessage)
	jsonData, ok := msg.Data.([]byte)
	if !ok {
		if data, err := json.Marshal(msg.Data); err != nil {
			return err
		} else {
			jsonData = []byte(data)
		}
	}

	if msg.Status == "" {
		msg.Status = statusWaiting
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now()
	}
	if msg.RetryAt.IsZero() {
		msg.RetryAt = msg.CreatedAt
	}

	sql := fmt.Sprintf(`
	INSERT INTO %s
		(queue, data, status, created_at, try_count, retry_at)
	VALUES
	    (%s,    %s,   %s,     '%s',       %d,        '%s')
	RETURNING id
	`,
		table.name,
		quote(msg.Queue), quote(string(jsonData)), quote(msg.Status),
		msg.CreatedAt.Format(rfc3339Micro), msg.TryCount, msg.RetryAt.Format(rfc3339Micro),
	)
	ctx, cancel := sqlTimeout()
	defer cancel()
	if err := db.QueryRowContext(ctx, sql).Scan(&msg.Id); err != nil {
		return errs.Trace(err)
	}
	return nil
}

func (table *StdTable) CleanMessages(db *sql.DB) (int64, error) {
	sql := fmt.Sprintf(`
	DELETE FROM %s
	WHERE status = '%s' AND retry_at < '%s'
	`,
		table.name, statusDone, time.Now().Add(-table.keep).Format(rfc3339Micro),
	)
	if result, err := db.Exec(sql); err != nil {
		return 0, errs.Trace(err)
	} else if n, err := result.RowsAffected(); err != nil {
		return 0, errs.Trace(err)
	} else {
		return n, nil
	}
}

func (table *StdTable) Name() string {
	return table.name
}

func sqlTimeout() (context.Context, func()) {
	return context.WithTimeout(context.Background(), 10*time.Second)
}

func checkAffectedOne(result sql.Result) error {
	if n, err := result.RowsAffected(); err != nil {
		return errs.Trace(err)
	} else if n != 1 {
		return errs.Tracef("affected %d rows", n)
	}
	return nil
}

// quote a string, removing all zero byte('\000') in it.
func quote(s string) string {
	s = strings.Replace(s, "'", "''", -1)
	s = strings.Replace(s, "\000", "", -1)
	return "'" + s + "'"
}
