package incomming

import (
	"notifier/config"
	"sync"
)

var (
	once          sync.Once
	queueInstance MsgsQueue
)

func GetQueue() MsgsQueue {
	return queueInstance
}

func Initialization() {
	once.Do(func() {
		conf := config.GetInstance()
		queue := NewMemoryQueue(conf.IncomingQueueSize)
		queueInstance = queue
	})
}
