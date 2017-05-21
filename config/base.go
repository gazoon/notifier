package config

import (
	"encoding/json"
	"github.com/pkg/errors"
	"io/ioutil"
	"flag"
)

type BaseConfig struct {
	LogLevel    string `json:"log_level"`
	ServiceName string `json:"service_name"`
	ServerID    string `json:"server_id"`
}

type DatabaseSettings struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Database string `json:"database"`
	Password string `json:"password"`
	Timeout  int    `json:"timeout"`
	PoolSize int    `json:"pool_size"`
}

type DatabaseQueue struct {
	DatabaseSettings `json:",inline"`
	FetchDelay       int `json:"fetch_delay"`
}

type TelegramSettings struct {
	APIToken    string `json:"api_token"`
	BotName     string `json:"bot_name"`
	HttpTimeout int    `json:"http_timeout"`
	Retries     int    `json:"retries"`
}

type TelegramPolling struct {
	PollTimeout int `json:"poll_timeout"`
	ChannelSize int `json:"channel_size"`
	RetryDelay  int `json:"retry_delay"`
}

func FromJSONFile(path string, config interface{}) error {
	if path == "" {
		return errors.New("empty config path")
	}
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return errors.Wrap(err, "cannot read config file")
	}
	err = json.Unmarshal(data, config)
	if err != nil {
		return errors.Wrap(err, "cannot parse json file")
	}
	return nil
}

func FromCmdArgs(confPath *string) {
	flag.StringVar(confPath, "conf", "conf.json", "Path to the config file")
}