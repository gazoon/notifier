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
	MongoNotification *DatabaseQueue    `json:"mongo_notification"`
	MongoMessages     *DatabaseQueue    `json:"mongo_messages"`
	Telegram          *TelegramSettings `json:"telegram"`
	TelegramPolling   *TelegramPolling  `json:"telegram_polling"`
	Logging           *Logging          `json:"logging"`
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
