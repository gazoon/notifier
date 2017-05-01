package config

import (
	"encoding/json"
	"github.com/pkg/errors"
	"io/ioutil"
	"sync"
)

var (
	once sync.Once
)

type NeoSettings struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Timeout  int    `json:"timeout"`
	PoolSize int    `json:"pool_size"`
}

type TelegramSettings struct {
	APIToken    string `json:"api_token"`
	BotName     string `json:"bot_name"`
	PullTimeout int    `json:"pull_timeout"`
	ChannelSize int    `json:"channel_size"`
	RetryDelay  int    `json:"retry_delay"`
}

var (
	LogLevel    string
	WorkersNum  int
	Neo         *NeoSettings
	Telegram    *TelegramSettings
	ServiceName string
	ServerID    string
)

func Initialization(configPath string) {
	once.Do(func() {
		if configPath == "" {
			panic("Empty config path")
		}
		data, err := ioutil.ReadFile(configPath)
		if err != nil {
			panic(errors.WithMessage(err, "cannot read config file"))
		}
		config := struct {
			LogLevel    string            `json:"log_level"`
			Neo         *NeoSettings      `json:"neo"`
			Telegram    *TelegramSettings `json:"telegram"`
			WorkersNum  int               `json:"workers_num"`
			APIToken    string            `json:"api_token"`
			ServiceName string            `json:"service_name"`
			ServerID    string            `json:"server_id"`
		}{}
		err = json.Unmarshal(data, &config)
		if err != nil {
			panic(errors.WithMessage(err, "cannot parse config"))
		}

		Neo = config.Neo
		Telegram = config.Telegram
		LogLevel = config.LogLevel
		WorkersNum = config.WorkersNum
		ServiceName = config.ServiceName
		ServerID = config.ServerID
	})
}
