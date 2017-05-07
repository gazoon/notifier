package sender

import (
	"github.com/pkg/errors"
	"notifier/config"
	"sync"
)

var (
	once           sync.Once
	senderInstance Sender
)

func GetInstance() Sender {
	return senderInstance
}

func Initialization() {
	once.Do(func() {
		conf := config.GetInstance()
		gLogger.Info("Initializing sender instance")
		sender, err := NewTelegramSender(conf.Telegram.APIToken, conf.TelegramSender.HttpTimeout)
		if err != nil {
			panic(errors.Wrap(err, "cannot initialize telegram sender"))
		}
		senderInstance = sender
	})
}
