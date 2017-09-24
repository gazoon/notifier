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
	config.BaseConfig          `mapstructure:",squash" json:",inline"`
	NotifyYourself             bool                     `mapstructure:"notify_yourself"json:"notify_yourself"`
	Neo                        *config.DatabaseSettings `mapstructure:"neo" json:"neo"`
	MongoNotification          *config.MongoQueue       `mapstructure:"mongo_notification" json:"mongo_notification"`
	MongoMessages              *config.MongoQueue       `mapstructure:"mongo_messages" json:"mongo_messages"`
	MongoNotificationsRegistry *config.MongoDBSettings  `mapstructure:"mongo_registry" json:"mongo_registry"`
	Telegram                   *config.TelegramSettings `mapstructure:"telegram" json:"telegram"`
	TelegramPolling            *config.TelegramPolling  `mapstructure:"telegram_polling" json:"telegram_polling"`
	Logging                    *config.Logging          `mapstructure:"logging" json:"logging"`
	GoogleAPI                  *config.GoogleAPI        `mapstructure:"google_api" json:"google_api"`
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
