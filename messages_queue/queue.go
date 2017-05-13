package msgsqueue

import (
	"context"
	"notifier/logging"
	"notifier/models"
	"sync"
	"time"
)

var (
	gLogger = logging.WithPackage("incoming_queue")
)

type Producer interface {
	Put(ctx context.Context, msg *models.Message) error
}

type Consumer interface {
	GetNext() (Message, bool)
	StopGivingMsgs()
}

type Message interface {
	Payload() *models.Message
	Ack()
}

type InMemoryQueue struct {
	storage     []*models.Message
	canGiveMsgs bool
	mx          sync.Mutex
}

func NewInMemory() *InMemoryQueue {
	return &InMemoryQueue{canGiveMsgs: true}
}

func (mq *InMemoryQueue) GetNext() (Message, bool) {
	for {
		if !mq.canGiveMsgs {
			return nil, false
		}
		mq.mx.Lock()
		if len(mq.storage) == 0 {
			mq.mx.Unlock()
			const fetch_delay = 10
			//gLogger.Debugf("List with messages is empty, wait %d milliseconds before the next attempt", fetch_delay)
			time.Sleep(time.Duration(fetch_delay) * time.Millisecond)
			continue
		}
		elem := mq.storage[0]
		mq.storage = mq.storage[1:]
		mq.mx.Unlock()
		return &regularMsg{elem}, true
	}
}

func (mq *InMemoryQueue) Put(ctx context.Context, msg *models.Message) error {
	mq.mx.Lock()
	defer mq.mx.Unlock()
	logger := logging.FromContextAndBase(ctx, gLogger)
	logger.Info("Append msg to the tail of the list")
	mq.storage = append(mq.storage, msg)
	return nil
}

func (mq *InMemoryQueue) StopGivingMsgs() {
	mq.canGiveMsgs = false
}

type regularMsg struct {
	payload *models.Message
}

func (rm *regularMsg) Payload() *models.Message {
	return rm.payload
}

func (rm *regularMsg) Ack() {}
