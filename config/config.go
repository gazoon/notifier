package config

import (
	"sync"

	"flag"
	"github.com/pkg/errors"
	"notifier/libs/config"
)

var (
	once     sync.Once
	instance *ServiceConfig
)

type ServiceConfig struct {
	config.BaseConfig
	BotWorkersNum     int                      `json:"bot_workers_num"`
	SenderWorkerNum   int                      `json:"sender_workers_num"`
	NotifyYourself    bool                     `json:"notify_yourself"`
	Neo               *config.DatabaseSettings `json:"neo"`
	MongoNotification *config.DatabaseQueue    `json:"mongo_notification"`
	MongoMessages     *config.DatabaseQueue    `json:"mongo_messages"`
	MongoRegistry     *config.DatabaseSettings `json:"mongo_registry"`
	Telegram          *config.TelegramSettings `json:"telegram"`
	TelegramPolling   *config.TelegramPolling  `json:"telegram_polling"`
	Logging           *config.Logging          `json:"logging"`
	GoogleAPI         *config.GoogleAPI        `json:"google_api"`
}

func Initialization(configPath string) {
	once.Do(func() {
		instance = &ServiceConfig{}
		err := config.FromJSONFile(configPath, instance)
		if err != nil {
			panic(errors.Wrap(err, "cannot load json config"))
		}
	})
}

func GetInstance() *ServiceConfig {
	return instance
}

func FromCmdArgs(confPath *string) {
	flag.StringVar(confPath, "conf", "conf.json", "Path to the config file")
}
