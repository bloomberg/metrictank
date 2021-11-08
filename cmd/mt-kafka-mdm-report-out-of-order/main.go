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

	outOfOrderGroupedByName := map[string]int{}
	duplicatesGroupedByName := map[string]int{}
	outOfOrderGroupedByTag := map[string]int{}
	duplicatesGroupedByTag := map[string]int{}
	inputOOOFinder := newInputOOOFinder(
		flags.Prefix,
		flags.Substr,
		flags.PartitionFrom,
		flags.PartitionTo,
		flags.ReorderWindow,
		flags.GroupByName,
		&outOfOrderGroupedByName,
		&duplicatesGroupedByName,
		flags.GroupByTag,
		&outOfOrderGroupedByTag,
		&duplicatesGroupedByTag,
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

	log.Info("todo out of order")
	if flags.GroupByName {
		log.Info("grouped by name:")
		for key, value := range outOfOrderGroupedByName {
			log.Infof("name=%q count=%d", key, value)
		}
	}
	if flags.GroupByTag != "" {
		log.Info("grouped by tag:")
		for key, value := range outOfOrderGroupedByTag {
			log.Infof("tag=%q count=%d", key, value)
		}
	}

	// todo rework this a bit
	// if enabled or always on
	log.Info("todo duplicates")
	if flags.GroupByName {
		log.Info("grouped by name:")
		for key, value := range duplicatesGroupedByName {
			log.Infof("name=%q count=%d", key, value)
		}
	}
	if flags.GroupByTag != "" {
		log.Info("grouped by tag:")
		for key, value := range duplicatesGroupedByTag {
			log.Infof("tag=%q count=%d", key, value)
		}
	}
}
