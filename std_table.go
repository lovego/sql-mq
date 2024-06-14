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
	StatusWaiting = "waiting"
	StatusDone    = "done"
	StatusGivenUp = "givenUp"

	Rfc3339Micro = "2006-01-02T15:04:05.999999Z07:00"
)

type StdMessage struct {
	Id         int64
	Queue      string      // quene name
	Data       interface{} // data of any type
	Status     string
	CreatedAt  time.Time
	TriedCount uint16    // how many times have tried already.
	RetryAt    time.Time // next retry at when.
}

func (msg *StdMessage) QueueName() string {
	return msg.Queue
}

func (msg *StdMessage) GetId() int64 {
	return msg.Id
}

func (msg *StdMessage) SetId(id int64) {
	msg.Id = id
}

func (msg *StdMessage) ConsumeAt() time.Time {
	return msg.RetryAt
}

func (msg *StdMessage) TableSql(tableName string) string {
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	id            bigserial    NOT NULL PRIMARY KEY,
	queue         text         NOT NULL,
	status        text         NOT NULL,
	created_at    timestamptz  NOT NULL,
	tried_count   smallint     NOT NULL,
	retry_at      timestamptz  NOT NULL,
	data          jsonb        NOT NULL
);
`, tableName)
}

func (msg *StdMessage) TableIndexSql(tableName string) []string {
	return []string{fmt.Sprintf(
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS %s_queue_status_retry_at ON %s (queue, status, retry_at)`,
		strings.Replace(tableName, ".", "_", 1), tableName,
	)}
}

func (msg *StdMessage) ProduceSql(tableName string) (string, error) {
	jsonData, ok := msg.Data.([]byte)
	if !ok {
		if data, err := json.Marshal(msg.Data); err != nil {
			return "", err
		} else {
			jsonData = []byte(data)
		}
	}

	if msg.Status == "" {
		msg.Status = StatusWaiting
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now()
	}
	if msg.RetryAt.IsZero() {
		msg.RetryAt = msg.CreatedAt
	}

	return fmt.Sprintf(`
	INSERT INTO %s
		(queue, data, status, created_at, tried_count, retry_at)
	VALUES
	    (%s,    %s,   %s,     '%s',       %d,        '%s')
	RETURNING id
	`,
		tableName,
		Quote(msg.Queue), Quote(string(jsonData)), Quote(msg.Status),
		msg.CreatedAt.Format(Rfc3339Micro), msg.TriedCount, msg.RetryAt.Format(Rfc3339Micro),
	), nil
}

func (msg *StdMessage) EarliestMessageSql(tableName string, queues []string) string {
	sort.Strings(queues)
	return fmt.Sprintf(`
	SELECT id, queue, data, status, created_at, tried_count, retry_at
	FROM %s
	WHERE queue IN (%s) AND status = '%s'
	ORDER BY retry_at
	LIMIT 1
	FOR UPDATE SKIP LOCKED
	`, tableName, strings.Join(queues, ","), StatusWaiting)
}

func (msg *StdMessage) EarliestMessage(tx *sql.Tx, querysql string) (Message, error) {
	row := StdMessage{}
	ctx, cancel := sqlTimeout()
	defer cancel()
	if err := tx.QueryRowContext(ctx, querysql).Scan(
		&row.Id, &row.Queue, &row.Data, &row.Status, &row.CreatedAt, &row.TriedCount, &row.RetryAt,
	); err == sql.ErrNoRows {
		return nil, nil
	} else if err != nil {
		return nil, errs.Trace(err)
	}
	return &row, nil
}

// NewStdTable create a standard `sqlmq.Table` instance.
// db: db use to create table and index.
// name: database table name.
// keep: keep a successfully consumed message for how long before delete it.
func NewStdTable(db *sql.DB, name string, keep time.Duration, msgs ...Message) *StdTable {
	var msg Message
	if len(msgs) == 0 || msgs[0] == nil {
		msg = &StdMessage{}
	} else {
		msg = msgs[0]
	}
	createTable(db, msg.TableSql(name))
	createIndex(db, msg.TableIndexSql(name)...)
	if keep < 0 {
		keep = 24 * time.Hour
	}
	return &StdTable{name: name, keep: keep, msg: msg}
}

func createTable(db *sql.DB, createSql string) {
	ctx, cancel := sqlTimeout()
	defer cancel()
	if _, err := db.ExecContext(ctx, createSql); err != nil {
		panic(time.Now().Format(time.RFC3339Nano) + " " + err.Error())
	}
}

func createIndex(db *sql.DB, createSqls ...string) {
	ctx, cancel := sqlTimeout()
	defer cancel()
	for i := range createSqls {
		if _, err := db.ExecContext(ctx, createSqls[i]); err != nil {
			panic(time.Now().Format(time.RFC3339Nano) + " " + err.Error())
		}
	}
}

// StdTable is a standard `sqlmq.Table` implementation.
type StdTable struct {
	name               string
	keep               time.Duration
	queues             []string
	earliestMessageSql string
	mutex              sync.RWMutex
	msg                Message
}

func (table *StdTable) SetQueues(queues []string) {
	table.mutex.Lock()
	defer table.mutex.Unlock()
	table.queues = queues
	table.earliestMessageSql = ""
}

func (table *StdTable) EarliestMessage(tx *sql.Tx) (Message, error) {
	querysql := table.getEarliestMessageSql()
	return table.msg.EarliestMessage(tx, querysql)
}

func (table *StdTable) getEarliestMessageSql() string {
	table.mutex.RLock()
	if table.earliestMessageSql == "" {
		var queues []string
		for _, queue := range table.queues {
			queues = append(queues, Quote(queue))
		}

		sort.Strings(queues)
		querySql := table.msg.EarliestMessageSql(table.name, queues)
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
	SET status = '%s', tried_count = tried_count+1, retry_at = '%s'
	WHERE id = %d
	`,
		table.name,
		StatusDone, time.Now().Format(Rfc3339Micro),
		message.GetId(),
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
	SET tried_count = tried_count + 1,  retry_at = '%s'
	WHERE id = %d
	`,
		table.name,
		time.Now().Add(retryAfter).Format(Rfc3339Micro),
		message.GetId(),
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
	SET status = '%s', tried_count = tried_count + 1, retry_at = '%s'
	WHERE id = %d
	`,
		table.name,
		StatusGivenUp, time.Now().Format(Rfc3339Micro),
		message.GetId(),
	)
	ctx, cancel := sqlTimeout()
	defer cancel()
	if result, err := db.ExecContext(ctx, sql); err != nil {
		return errs.Trace(err)
	} else {
		return checkAffectedOne(result)
	}
}

// if ProduceMessage runs succussfully, message id is set in message.
func (table *StdTable) ProduceMessage(db DBOrTx, message Message) error {
	sql, err := message.ProduceSql(table.name)
	if err != nil {
		return err
	}
	ctx, cancel := sqlTimeout()
	defer cancel()
	var id int64
	if err := db.QueryRowContext(ctx, sql).Scan(&id); err != nil {
		return errs.Trace(err)
	}
	message.SetId(id)
	return nil
}

func (table *StdTable) CleanMessages(db *sql.DB) (int64, error) {
	sql := fmt.Sprintf(`
	DELETE FROM %s
	WHERE status = '%s' AND retry_at < '%s'
	`,
		table.name, StatusDone, time.Now().Add(-table.keep).Format(Rfc3339Micro),
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

// Quote a string, removing all zero byte('\000') in it.
func Quote(s string) string {
	s = strings.Replace(s, "'", "''", -1)
	s = strings.Replace(s, "\000", "", -1)
	return "'" + s + "'"
}
