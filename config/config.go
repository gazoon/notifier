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
	BotWorkersNum     int               `json:"bot_workers_num"`
	SenderWorkerNum   int               `json:"sender_workers_num"`
	NotifyYourself    bool              `json:"notify_yourself"`
	Neo               *DatabaseSettings `json:"neo"`
	MongoNotification *DatabaseSettings `json:"mongo_notification"`
	Telegram          *TelegramSettings `json:"telegram"`
	TelegramPolling   *TelegramPolling  `json:"telegram_polling"`
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
