package main

import (
	"flag"
	"notifier/core"
)

func main() {
	var confPath string
	flag.StringVar(&confPath, "conf", "conf.json", "Path to the config file")
	flag.Parse()
	core.Run(confPath)
}
