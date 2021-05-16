package sqlmq

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
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

func NewStdTable(db *sql.DB, name string) *StdTable {
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
CREATE INDEX IF NOT EXISTS %s_retry_at ON %s (retry_at)
WHERE status = '%s'
`, name, name, name, statusWaiting,
	)
	if _, err := db.Exec(createSql); err != nil {
		panic(time.Now().Format(time.RFC3339Nano) + " " + err.Error())
	}
	return &StdTable{name: name}
}

type StdTable struct {
	name               string
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
	if err := tx.QueryRow(querysql).Scan(
		&row.Id, &row.Queue, &row.Data, &row.Status, &row.CreatedAt, &row.TryCount, &row.RetryAt,
	); err == sql.ErrNoRows {
		return nil, nil
	} else if err != nil {
		return nil, err
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
	_, err := tx.Exec(sql)
	return err
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
	_, err := db.Exec(sql)
	return err
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
	_, err := db.Exec(sql)
	return err
}

// if ProduceMessage runs succussfully, message id is set message(which is *StdMessage).
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
	return db.QueryRow(sql).Scan(&msg.Id)
}

func (table *StdTable) Name() string {
	return table.name
}

// quote a string, removing all zero byte('\000') in it.
func quote(s string) string {
	s = strings.Replace(s, "'", "''", -1)
	s = strings.Replace(s, "\000", "", -1)
	return "'" + s + "'"
}
