package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"

	"microservice/internal"
)

func main() {
	os.Exit(realMain())
}

func realMain() int {
	var (
		err error
		k   *internal.Kafka
	)

	if k, err = internal.New(logrus.New()); err != nil {
		panic(err)
		return 1
	}
	defer k.Shutdown()

	signalEvents := make(chan os.Signal, 1)
	signal.Notify(signalEvents, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	for {
		select {
		case <-signalEvents:
			return 0
		case <-k.Done():
			return 1
		}
	}

	return 0
}
