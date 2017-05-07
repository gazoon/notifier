package config

import (
	"sync"

	"github.com/pkg/errors"
)

var (
	once           sync.Once
	configInstance *ServiceConfig
)

type ServiceConfig struct {
	BaseConfig
	WorkersNum        int               `json:"workers_num"`
	IncomingQueueSize int               `json:"incoming_queue_size"`
	Neo               *NeoSettings      `json:"neo"`
	Telegram          *TelegramSettings `json:"telegram"`
	TelegramPolling   *TelegramPolling  `json:"telegram_polling"`
	TelegramSender    *TelegramSender   `json:"telegram_sender"`
}

func Initialization(configPath string) {
	once.Do(func() {
		configInstance = &ServiceConfig{}
		err := FromJSONFile(configPath, configInstance)
		if err != nil {
			panic(errors.Wrap(err, "cannot load json config"))
		}
	})
}

func GetInstance() *ServiceConfig {
	return configInstance
}
