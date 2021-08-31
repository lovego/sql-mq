package sqlmq

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/lovego/logger"
)

func (mq *SqlMQ) Consume() {
	if err := mq.validate(); err != nil {
		panic(time.Now().Format(time.RFC3339Nano) + " " + err.Error())
	}
	if mq.CleanInterval > 0 {
		go mq.clean()
	}

	idleWait, errorWait := mq.getWaitTime()
	if mq.debug {
		for {
			mq.sleep.ClearAwakeAt() // for subsequent sleep.AwakeAtEalier() calls.
			var wait = mq.consume(idleWait, errorWait)
			logf("consumed.")
			mq.NotifyConsumeAt(time.Now().Add(wait), "sleep "+wait.String())
			logf("awaken for %v", mq.sleep.Run())
		}
	} else {
		for {
			mq.sleep.ClearAwakeAt() // for subsequent sleep.AwakeAtEalier() calls.
			var wait = mq.consume(idleWait, errorWait)
			mq.NotifyConsumeAt(time.Now().Add(wait), nil)
			mq.sleep.Run()
		}
	}
}

func (mq *SqlMQ) consume(idleWait, errorWait time.Duration) time.Duration {
	if mq.noQueues() {
		return idleWait
	}
	for {
		if wait, err := mq.consumeOne(idleWait); err != nil {
			mq.Logger.Error(err)
			return errorWait
		} else if wait > 0 {
			if wait > idleWait {
				wait = idleWait
			}
			return wait
		}
	}
}

func (mq *SqlMQ) consumeOne(idleWait time.Duration) (wait time.Duration, err error) {
	mq.concurrencyLimit() <- struct{}{}
	tx, cancel, err := mq.beginTx()
	if err != nil {
		<-mq.concurrencyLimit()
		return
	}

	msg, err := mq.Table.EarliestMessage(tx)
	if msg != nil {
		wait = time.Until(msg.ConsumeAt())
	} else {
		wait = idleWait
	}
	if wait > 0 || err != nil {
		if err2 := tx.Rollback(); err2 != nil {
			mq.Logger.Error(err2)
		}
		cancel()
		<-mq.concurrencyLimit()
		return
	}

	var retryAfter time.Duration
	var handleErr error

	go mq.Logger.Record(func(ctx context.Context) error {
		retryAfter, handleErr = mq.handle(ctx, cancel, tx, msg)
		return handleErr
	}, nil, func(f *logger.Fields) {
		f.With("message", msg)
		if handleErr != nil {
			f.With("retryAfter", retryAfter.String())
		}
		<-mq.concurrencyLimit()
	})

	return
}

func (mq *SqlMQ) handle(ctx context.Context, cancel func(), tx *sql.Tx, msg Message) (
	retryAfter time.Duration, err error,
) {
	var canCommit bool
	var notifyConsumeAt time.Time
	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			if canCommit {
				if err2 := tx.Commit(); err2 != nil {
					mq.Logger.Error(err2)
				} else if !notifyConsumeAt.IsZero() {
					mq.NotifyConsumeAt(notifyConsumeAt, "retry") // must be after released lock.
				}
			} else {
				if err2 := tx.Rollback(); err2 != nil {
					mq.Logger.Error(err2)
				}
			}
		}
		cancel()
	}()

	handler, err := mq.handlerOf(msg)
	if err == nil {
		if retryAfter, canCommit, err = handler(ctx, tx, msg); err == nil {
			err = mq.Table.MarkSuccess(tx, msg)
		} else if canCommit {
			notifyConsumeAt = mq.markFail(tx, msg, retryAfter, false)
		} else {
			// Do this before transaction released the "FOR UPDATE" lock.
			go mq.markFail(mq.DB, msg, retryAfter, true)
			// Wait the goroutine above to be ready to preempt the lock before rollback release the lock.
			// Reduce the rate that `EarliestMessage` got the lock and consume this message again.
			time.Sleep(100 * time.Millisecond)
		}
	} else {
		retryAfter, canCommit = time.Minute, true
		notifyConsumeAt = mq.markFail(tx, msg, retryAfter, false)
	}
	return
}

func (mq *SqlMQ) markFail(
	db DBOrTx, msg Message, retryAfter time.Duration, notifyConsume bool,
) time.Time {
	if retryAfter >= 0 {
		if err := mq.Table.MarkRetry(db, msg, retryAfter); err != nil {
			mq.Logger.Error(err)
		} else if notifyConsume {
			mq.NotifyConsumeAt(time.Now().Add(retryAfter), "retry") // must be after released lock.
		} else {
			return time.Now().Add(retryAfter)
		}
	} else {
		if err := mq.Table.MarkGivenUp(db, msg); err != nil {
			mq.Logger.Error(err)
		}
	}
	return time.Time{}
}

func (mq *SqlMQ) beginTx() (*sql.Tx, func(), error) {
	txTimeout := mq.TxTimeout
	if txTimeout <= 0 {
		txTimeout = time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), txTimeout)
	tx, err := mq.DB.BeginTx(ctx, nil)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	return tx, cancel, err
}

func (mq *SqlMQ) getWaitTime() (idleWait, errorWait time.Duration) {
	idleWait, errorWait = mq.IdleWait, mq.ErrorWait
	if idleWait <= 0 {
		idleWait = time.Minute
	}
	if errorWait <= 0 {
		errorWait = time.Minute
	}
	return
}

func (mq *SqlMQ) concurrencyLimit() chan struct{} {
	if mq.consumeConcurrency == nil {
		n := mq.ConsumeConcurrency
		if n <= 0 {
			n = 10
		}
		mq.consumeConcurrency = make(chan struct{}, n)
	}
	return mq.consumeConcurrency
}

func (mq *SqlMQ) clean() {
	for {
		var cleaned int64
		var err error
		mq.Logger.Record(func(ctx context.Context) error {
			cleaned, err = mq.Table.CleanMessages(mq.DB)
			return err
		}, nil, func(f *logger.Fields) {
			f.With("cleaned", cleaned)
		})
		time.Sleep(mq.CleanInterval)
	}
}

func logf(msg string, args ...interface{}) {
	if len(args) > 0 {
		msg = fmt.Sprintf(msg, args...)
	}
	fmt.Fprintln(os.Stderr, time.Now().Format("2006-01-02T15:04:05.000Z07:00")+" "+msg)
}
