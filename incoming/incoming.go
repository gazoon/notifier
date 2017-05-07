package incomming

import (
	"notifier/models"
)

type MsgsQueue interface {
	GetNext() Message
	Put(msg *models.Message) error
}

type Message interface {
	Payload() *models.Message
	Ack()
}

type inMemoryQueue struct {
	storage chan *models.Message
}

func NewMemoryQueue(maxSize int) MsgsQueue {
	ch := make(chan *models.Message, maxSize)
	return &inMemoryQueue{ch}
}

func (mq *inMemoryQueue) GetNext() Message {
	return &regularMsg{<-mq.storage}
}

func (mq *inMemoryQueue) Put(msg *models.Message) error {
	mq.storage <- msg
	return nil
}

type regularMsg struct {
	payload *models.Message
}

func (rm *regularMsg) Payload() *models.Message {
	return rm.payload
}

func (rm *regularMsg) Ack() {}
