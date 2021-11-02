package main

import (
	"context"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/grafana/globalconf"
	"github.com/grafana/metrictank/idx/cassandra"
	inKafkaMdm "github.com/grafana/metrictank/input/kafkamdm"
	"github.com/grafana/metrictank/logger"
	"github.com/grafana/metrictank/schema"
	log "github.com/sirupsen/logrus"
)

func configureLogging() {
	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)
}

func configureKafkaMdm(configurationFile string) *inKafkaMdm.KafkaMdm {
	// Only try and parse the conf file if it exists
	path := ""
	if _, err := os.Stat(configurationFile); err == nil {
		path = configurationFile
	}
	conf, err := globalconf.NewWithOptions(&globalconf.Options{
		Filename:  path,
		EnvPrefix: "MT_",
	})
	if err != nil {
		log.Fatalf("error with configuration file: %s", err.Error())
		os.Exit(1)
	}

	inKafkaMdm.ConfigSetup()

	conf.Parse()
	inKafkaMdm.Enabled = true // config may have had it disabled
	inKafkaMdm.ConfigProcess("mt-kafka-mdm-report-out-of-order" + strconv.Itoa(rand.Int()))

	return inKafkaMdm.New()
}

func loadMetricDefinitionsFromCassandra(partitionFrom int, partitionTo int) Tracker {
	cassandra.CliConfig.Enabled = true
	cassandraIndex := cassandra.New(cassandra.CliConfig)
	err := cassandraIndex.InitBare()
	if err != nil {
		log.Fatalf("error initializing cassandra index: %s", err.Error())
		os.Exit(1)
	}

	metricDefinitionSlice := make([]schema.MetricDefinition, 0)
	for partition := partitionFrom; (partitionTo == -1 && partition == partitionFrom) || (partitionTo > 0 && partition < partitionTo); partition++ {
		metricDefinitionSlice = cassandraIndex.LoadPartitions([]int32{int32(partition)}, metricDefinitionSlice, time.Now())
	}
	metricDefinitions := Tracker{}
	for _, def := range metricDefinitionSlice {
		metricDefinitions[def.Id] = Track{
			Name: def.Name,
			Tags: def.Tags,
		}
	}

	return metricDefinitions
}

func main() {
	configureLogging()

	applicationFlags := ParseFlags()

	kafkaMdm := configureKafkaMdm(applicationFlags.ConfFile)

	groupedByName := map[string]int{}
	groupedByTag := map[string]int{}
	inputOOOFinder := newInputOOOFinder(
		applicationFlags.GraceDuration,
		applicationFlags.Prefix,
		applicationFlags.Substr,
		applicationFlags.DoUnknownMP,
		loadMetricDefinitionsFromCassandra(applicationFlags.PartitionFrom, applicationFlags.PartitionTo),
		applicationFlags.GroupByName,
		&groupedByName,
		applicationFlags.GroupByTag,
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
	case <-time.After(applicationFlags.RunDuration):
		log.Infof("Finished scanning")
	}
	kafkaMdm.Stop()

	if applicationFlags.GroupByName {
		log.Info("grouped by name:")
		for key, value := range groupedByName {
			log.Infof("%s : %d", key, value)
		}
	}
	if applicationFlags.GroupByTag != "" {
		log.Info("grouped by tag:")
		for key, value := range groupedByTag {
			log.Infof("%s : %d", key, value)
		}
	}
}
