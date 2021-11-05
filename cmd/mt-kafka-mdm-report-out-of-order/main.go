package main

import (
	"context"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	inKafkaMdm "github.com/grafana/metrictank/input/kafkamdm"
	"github.com/grafana/metrictank/logger"
	log "github.com/sirupsen/logrus"
)

func configureLogging() {
	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)
}

func main() {
	configureLogging()

	flags := ParseFlags()

	inKafkaMdm.ConfigProcess("mt-kafka-mdm-report-out-of-order" + strconv.Itoa(rand.Int()))
	kafkaMdm := inKafkaMdm.New()

	groupedByName := map[string]int{}
	groupedByTag := map[string]int{}
	inputOOOFinder := newInputOOOFinder(
		flags.Prefix,
		flags.Substr,
		flags.PartitionFrom,
		flags.PartitionTo,
		flags.ReorderWindow,
		flags.GroupByName,
		&groupedByName,
		flags.GroupByTag,
		&groupedByTag,
	)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	kafkaMdm.Start(inputOOOFinder, cancel)
	select {
	case sig := <-sigChan:
		log.Infof("Received signal %q. Shutting down", sig)
	case <-ctx.Done():
		log.Info("Mdm input plugin signalled a fatal error. Shutting down")
	case <-time.After(flags.RunDuration):
		log.Infof("Finished scanning")
	}
	kafkaMdm.Stop()

	if flags.GroupByName {
		log.Info("grouped by name:")
		for key, value := range groupedByName {
			log.Infof("name=%s count=%d", key, value)
		}
	}
	if flags.GroupByTag != "" {
		log.Info("grouped by tag:")
		for key, value := range groupedByTag {
			log.Infof("tag=%s count=%d", key, value)
		}
	}
}
