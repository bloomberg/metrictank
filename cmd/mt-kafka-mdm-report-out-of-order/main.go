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
		uint32(flags.ReorderWindow),
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

	if flags.GroupByName {
		log.Info("out-of-order metrics grouped by name:")
		for key, value := range outOfOrderGroupedByName {
			log.Infof("out-of-order name=%q count=%d", key, value)
		}

		log.Info("duplicate metrics grouped by name:")
		for key, value := range duplicatesGroupedByName {
			log.Infof("duplicate name=%q count=%d", key, value)
		}
	}
	if flags.GroupByTag != "" {
		log.Info("out-of-order metrics grouped by tag:")
		for key, value := range outOfOrderGroupedByTag {
			log.Infof("out-of-order tag=%q count=%d", key, value)
		}

		log.Info("duplicate metrics grouped by tag:")
		for key, value := range duplicatesGroupedByTag {
			log.Infof("duplicate tag=%q count=%d", key, value)
		}
	}
}
