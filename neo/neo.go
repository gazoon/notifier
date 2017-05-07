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
		neoConf := config.GetInstance().Neo
		gLogger.Info("Initializing neo instance")
		db, err = NewClient(neoConf.Host, neoConf.Port, neoConf.User, neoConf.Password, neoConf.Timeout,
			neoConf.PoolSize)
		if err != nil {
			panic(errors.Wrap(err, "neo initialization failed"))
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
