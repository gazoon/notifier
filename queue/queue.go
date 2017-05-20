package queue

import (
	"sync/atomic"
	"time"
)

type BaseConsumer struct {
	readClosed int32
	fetchDelay time.Duration
}

func NewBaseConsumer(fetchDelay int) *BaseConsumer {
	return &BaseConsumer{fetchDelay: time.Millisecond * time.Duration(fetchDelay)}
}

func (bc *BaseConsumer) StopGivingMsgs() {
	atomic.StoreInt32(&bc.readClosed, 1)
}

func (bc *BaseConsumer) FetchLoop(fetch func() bool) bool {
	for {
		if atomic.LoadInt32(&bc.readClosed) == 1 {
			return false
		}
		isMsgFetched := fetch()
		if !isMsgFetched {
			time.Sleep(bc.fetchDelay)
			continue
		}
		return true
	}
}
