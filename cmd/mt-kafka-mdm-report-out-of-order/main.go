package main

import (
	"context"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
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

func filter(tracker Tracker, prefix string, substr string) {
	for key, track := range tracker {
		if prefix != "" && !strings.HasPrefix(track.Name, prefix) {
			delete(tracker, key)
		}
		if substr != "" && !strings.Contains(track.Name, substr) {
			delete(tracker, key)
		}
	}
}

func aggregateAndLog(tracker Tracker, groupByName bool, groupByTag string) {
	count := 0
	outOfOrderCount := 0
	duplicateCount := 0
	for _, track := range tracker {

		count += track.Count
		outOfOrderCount += track.OutOfOrderCount
		duplicateCount += track.DuplicateCount
	}

	log.Infof("total metric points count=%d", count)
	log.Infof("total out-of-order metric points count=%d", outOfOrderCount)
	log.Infof("total duplicate metric points count=%d", duplicateCount)

	if groupByName {
		aggregatedByName := aggregateByName(tracker)
		log.Info("out-of-order metric points grouped by name:")
		for name, aggregate := range aggregatedByName {
			if aggregate.OutOfOrderCount > 0 {
				log.Infof("out-of-order metric points for name=%q count=%d percentName=%f percentTotalOutOfOrder=%f", name, aggregate.OutOfOrderCount, float64(aggregate.OutOfOrderCount)/float64(aggregate.Count)*100, float64(aggregate.OutOfOrderCount)/float64(outOfOrderCount)*100)
			}
		}

		log.Info("duplicate metric points grouped by name:")
		for name, aggregate := range aggregatedByName {
			if aggregate.DuplicateCount > 0 {
				log.Infof("duplicate metric points for name=%q count=%d percentName=%f percentTotalDuplicate=%f", name, aggregate.DuplicateCount, float64(aggregate.DuplicateCount)/float64(aggregate.Count)*100, float64(aggregate.DuplicateCount)/float64(duplicateCount)*100)
			}
		}
	}

	if groupByTag != "" {
		aggregatedByTag := aggregateByTag(tracker, groupByTag)
		log.Infof("out-of-order metric points grouped by tag=%q:", groupByTag)
		for tag, aggregate := range aggregatedByTag {
			if aggregate.OutOfOrderCount > 0 {
				log.Infof("out-of-order metric points for tag=%q value=%q count=%d percentTag=%f percentTotalOutOfOrder=%f", groupByTag, tag, aggregate.OutOfOrderCount, float64(aggregate.OutOfOrderCount)/float64(aggregate.Count)*100, float64(aggregate.OutOfOrderCount)/float64(outOfOrderCount)*100)
			}
		}

		log.Infof("duplicate metric points grouped by tag=%q:", groupByTag)
		for tag, aggregate := range aggregatedByTag {
			if aggregate.DuplicateCount > 0 {
				log.Infof("duplicate metric points for tag=%q value=%q count=%d percentTag=%f percentTotalDuplicate=%f", groupByTag, tag, aggregate.DuplicateCount, float64(aggregate.DuplicateCount)/float64(aggregate.Count)*100, float64(aggregate.DuplicateCount)/float64(duplicateCount)*100)
			}
		}
	}
	return
}

func main() {
	configureLogging()

	flags := ParseFlags()

	inKafkaMdm.ConfigProcess("mt-kafka-mdm-report-out-of-order" + strconv.Itoa(rand.Int()))
	kafkaMdm := inKafkaMdm.New()

	inputOOOFinder := newInputOOOFinder(
		flags.PartitionFrom,
		flags.PartitionTo,
		uint32(flags.ReorderWindow),
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

	tracker := inputOOOFinder.Tracker()
	filter(tracker, flags.Prefix, flags.Substr)
	aggregateAndLog(tracker, flags.GroupByName, flags.GroupByTag)
}
