package sqlmq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/lovego/logger"
)

type SqlMQ struct {
	DB     *sql.DB
	Table  Table
	Logger *logger.Logger

	// If no message is available for consuming, wait how long before try to fetch message again.
	// If IdleWait <= 0, the default value one minute is used.
	IdleWait time.Duration
	// If encounter an error when fetching message, wait how long before try to fetch message again.
	// If ErrorWait <= 0, the default value one minute is used.
	ErrorWait time.Duration
	// Transaction timeout for message fecthing and handling.
	// If TxTimeout <= 0, the default value one minute is used.
	TxTimeout time.Duration

	// The time interval to clean successfully consumed messages.
	CleanInterval time.Duration

	queues map[string]Handler
	mutex  sync.RWMutex

	// trigger consume right now
	consumeNotify chan struct{}
}

type DBOrTx interface {
	QueryContext(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, sql string, args ...interface{}) *sql.Row
	ExecContext(ctx context.Context, sql string, args ...interface{}) (sql.Result, error)
}

type Table interface {
	// Set the queues for EarliestMessage. This method must be concurrency safe.
	SetQueues(queues []string)

	// Get the earliest message in the "SetQueues" which have not been "MarkSuccess".
	// The "earliest" means smallest "ConsumeAt".
	// If no such message, return a nil interface.
	// The tx must be used to exclusively lock (SELECT FOR UPDATE) the returned message.
	// Don't commit or rollback the tx.
	EarliestMessage(tx *sql.Tx) (Message, error)

	// Mark a message as consumed successfully.
	// The tx must be used to update the message. Don't commit or rollback the tx.
	MarkSuccess(tx *sql.Tx, msg Message) error

	// mark a message should be retried after a time period
	MarkRetry(db DBOrTx, msg Message, retryAfter time.Duration) error

	// mark a message as given up
	MarkGivenUp(db DBOrTx, msg Message) error

	// produce a message.
	ProduceMessage(db DBOrTx, msg Message) error
	// clean successfully consumed messages, may keep a duration after consumed for debugging.
	// return the number of cleaned messages.
	CleanMessages(db *sql.DB) (int64, error)
}

type Message interface {
	QueueName() string
	// At which time the message should be consumed(either first time or retry).
	ConsumeAt() time.Time
}

// On successful handling, a nil error should be returned, retryAfter and canCommit is ignored.
// On failing handling, a non nil error should be returned, and retryAfter means:
// 1. if retryAfter is positive, means try again that time period later;
// 2. if retryAfter is zero,     means try again immediately;
// 3. if retryAfter is negative, means give up this message, don't try again.
// canCommit means when an error is returned, can the transaction be committed or must be rollbacked.
// If canCommit is false, this transaction is rollbacked, and another statements is executed to update retry time.
type Handler func(ctx context.Context, tx *sql.Tx, msg Message) (
	retryAfter time.Duration, canCommit bool, err error,
)

func (mq *SqlMQ) Register(queueName string, handler Handler) error {
	mq.mutex.RLock()
	existingHandler := mq.queues[queueName]
	mq.mutex.RUnlock()
	if existingHandler != nil {
		return fmt.Errorf("queue %s aready registerd", queueName)
	}

	mq.mutex.Lock()
	if mq.queues == nil {
		mq.queues = make(map[string]Handler)
	}
	mq.queues[queueName] = handler
	mq.mutex.Unlock()

	var queues = make([]string, 0, len(mq.queues))
	mq.mutex.RLock()
	for queue, handler := range mq.queues {
		if handler != nil {
			queues = append(queues, queue)
		}
	}
	mq.mutex.RUnlock()

	mq.Table.SetQueues(queues)
	mq.TriggerConsume()
	return nil
}

func (mq *SqlMQ) noQueues() bool {
	mq.mutex.RLock()
	defer mq.mutex.RUnlock()
	return len(mq.queues) == 0
}

func (mq *SqlMQ) handlerOf(msg Message) (Handler, error) {
	mq.mutex.RLock()
	defer mq.mutex.RUnlock()
	handler := mq.queues[msg.QueueName()]
	if handler == nil {
		return nil, errors.New("unknown queue: " + msg.QueueName())
	}
	return handler, nil
}

func (mq *SqlMQ) TriggerConsume() {
	select {
	case mq.consumeNotify <- struct{}{}:
	default:
	}
}

// Produce a meesage. tx can be nil.
func (mq *SqlMQ) Produce(tx *sql.Tx, msg Message) error {
	if _, err := mq.handlerOf(msg); err != nil {
		return err
	}
	var db DBOrTx = mq.DB
	if tx != nil {
		db = tx
	}
	if err := mq.Table.ProduceMessage(db, msg); err != nil {
		return err
	}
	mq.TriggerConsume()
	return nil
}

func (mq *SqlMQ) validate() error {
	if mq.DB == nil {
		return errors.New("SqlMQ.DB must not be nil.")
	}
	if mq.Table == nil {
		return errors.New("SqlMQ.Table must not be nil.")
	}
	if mq.Logger == nil {
		mq.Logger = logger.New(os.Stderr)
	}
	return nil
}
