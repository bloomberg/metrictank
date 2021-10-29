package main

import (
	"flag"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
)

type ApplicationFlags struct {
	flagSet *flag.FlagSet

	runDurationStr   string
	ConfFile         string
	PartitionFrom    int
	PartitionTo      int
	graceDurationStr string
	Prefix           string
	Substr           string
	DoUnknownMP      bool
	GroupByName      bool
	GroupByTag       string

	// after parsing
	RunDuration   time.Duration
	GraceDuration time.Duration
}

func NewApplicationFlags() *ApplicationFlags {
	var applicationFlags ApplicationFlags

	applicationFlags.flagSet = flag.NewFlagSet("application flags", flag.ExitOnError)
	applicationFlags.flagSet.StringVar(&applicationFlags.runDurationStr, "run-duration", "5m", "the duration of time to run the program")
	applicationFlags.flagSet.StringVar(&applicationFlags.ConfFile, "config", "/etc/metrictank/metrictank.ini", "configuration file path")
	applicationFlags.flagSet.IntVar(&applicationFlags.PartitionFrom, "partition-from", 0, "the partition to load the index from")
	applicationFlags.flagSet.IntVar(&applicationFlags.PartitionTo, "partition-to", -1, "load the index from all partitions up to this one (exclusive). If unset, only the partition defined with \"--partition-from\" is loaded from")
	applicationFlags.flagSet.StringVar(&applicationFlags.graceDurationStr, "grace-duration", "0s", "todo")
	applicationFlags.flagSet.StringVar(&applicationFlags.Prefix, "prefix", "", "only show metrics with a name that has this prefix")
	applicationFlags.flagSet.StringVar(&applicationFlags.Substr, "substr", "", "only show metrics with a name that has this substring")
	applicationFlags.flagSet.BoolVar(&applicationFlags.DoUnknownMP, "do-unknown-mp", true, "process MetricPoint messages for which no MetricData messages have been seen. If you use prefix/substr filter, this may report on metrics you wanted to filter out!")
	applicationFlags.flagSet.BoolVar(&applicationFlags.GroupByName, "group-by-name", false, "group out-of-order metrics by name")
	applicationFlags.flagSet.StringVar(&applicationFlags.GroupByTag, "group-by-tag", "", "group out-of-order metrics by the specified tag")

	return &applicationFlags
}

func (applicationFlags *ApplicationFlags) Parse(args []string) {
	err := applicationFlags.flagSet.Parse(args)
	if err != nil {
		log.Fatalf("failed to parse application flags %v: %s", args, err.Error)
		os.Exit(1)
	}

	applicationFlags.RunDuration, err = time.ParseDuration(applicationFlags.runDurationStr)
	if err != nil {
		log.Fatalf("failed to parse run duration %s: %s", applicationFlags.runDurationStr, err.Error)
		os.Exit(1)
	}
	applicationFlags.GraceDuration, err = time.ParseDuration(applicationFlags.graceDurationStr)
	if err != nil {
		log.Fatalf("failed to parse grace duration %s: %s", applicationFlags.graceDurationStr, err.Error)
		os.Exit(1)
	}

	if applicationFlags.GroupByName == false && applicationFlags.GroupByTag == "" {
		log.Fatalf("must specify one of -group-by-name or -group-by-tag")
		os.Exit(1)
	}
}
