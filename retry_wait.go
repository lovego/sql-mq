package sqlmq

import "time"

var stdRetryWaits = RetryWait{
	Waits: []time.Duration{
		time.Second,
		time.Minute,
		time.Hour,
		24 * time.Hour,
	},
}

func GetRetryWait(retriedCount uint16) time.Duration {
	return stdRetryWaits.Get(retriedCount)
}

type RetryWait struct {
	Waits []time.Duration
}

func (retry RetryWait) Get(retriedCount uint16) time.Duration {
	retried := int(retriedCount)
	if retried >= len(retry.Waits) {
		retried = len(retry.Waits) - 1
	}
	return retry.Waits[retried]
}
