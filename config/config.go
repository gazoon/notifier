package config

import (
	"sync"

	"github.com/gazoon/bot_libs/config"
)

var (
	once     sync.Once
	instance *ServiceConfig
)

type ServiceConfig struct {
	config.BaseConfig
	NotifyYourself             bool                     `json:"notify_yourself"`
	Neo                        *config.DatabaseSettings `json:"neo"`
	MongoNotification          *config.DatabaseQueue    `json:"mongo_notification"`
	MongoMessages              *config.DatabaseQueue    `json:"mongo_messages"`
	MongoNotificationsRegistry *config.DatabaseSettings `json:"mongo_registry"`
	Telegram                   *config.TelegramSettings `json:"telegram"`
	TelegramPolling            *config.TelegramPolling  `json:"telegram_polling"`
	Logging                    *config.Logging          `json:"logging"`
	GoogleAPI                  *config.GoogleAPI        `json:"google_api"`
}

func Initialization(configPath string) {
	once.Do(func() {
		instance = &ServiceConfig{}
		err := config.FromJSONFile(configPath, instance)
		if err != nil {
			panic(err)
		}
	})
}

func GetInstance() *ServiceConfig {
	return instance
}

func FromCmdArgs(confPath *string) {
	config.FromCmdArgs(confPath)
}
