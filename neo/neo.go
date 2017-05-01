package neo

import (
	"fmt"
	neoDriver "github.com/johnnadratowski/golang-neo4j-bolt-driver"
	"github.com/pkg/errors"
	"notifier/config"
	"sync"
)

var once = sync.Once{}

type NeoClient neoDriver.DriverPool

var DBInstance NeoClient

func Initialization() {
	once.Do(func() {
		connStr := buildConnectionStr()
		pool, err := neoDriver.NewDriverPool(connStr, config.Neo.PoolSize)
		if err != nil {
			panic(errors.WithMessage(err, "cannot initialize neo instance"))
		}
		DBInstance = NeoClient(pool)
	})
}

func buildConnectionStr() string {
	uri := fmt.Sprintf("bolt://%s:%s@%s:%d?timeout=%d", config.Neo.User, config.Neo.Password, config.Neo.Host,
		config.Neo.Port, config.Neo.Timeout)
	return uri
}
