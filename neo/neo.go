package neo

import (
	"notifier/config"
	"sync"

	"github.com/pkg/errors"
)

var (
	once = sync.Once{}
	db   NeoClient
)

func Initialization() {
	once.Do(func() {
		var err error
		db, err = NewClient(config.Neo.Host, config.Neo.Port, config.Neo.User, config.Neo.Password, config.Neo.Timeout,
			config.Neo.PoolSize)
		if err != nil {
			panic(errors.WithMessage(err, "neo initialization failed"))
		}
	})
}
func GetDBConn() (NeoConn, error) {
	conn, err := GetDB().GetConn()
	return conn, err
}

func GetDB() NeoClient {
	return db
}
