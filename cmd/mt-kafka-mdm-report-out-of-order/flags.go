package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/grafana/globalconf"
	"github.com/grafana/metrictank/idx/cassandra"
	inKafkaMdm "github.com/grafana/metrictank/input/kafkamdm"
	log "github.com/sirupsen/logrus"
)

type Flags struct {
	flagSet *flag.FlagSet

	runDurationStr   string
	Config           string
	PartitionFrom    int
	PartitionTo      int
	graceDurationStr string
	Prefix           string
	Substr           string
	GroupByName      bool
	GroupByTag       string

	// after parsing
	RunDuration   time.Duration
	GraceDuration time.Duration
}

func NewFlags() *Flags {
	var flags Flags

	flags.flagSet = flag.NewFlagSet("application flags", flag.ExitOnError)
	flags.flagSet.StringVar(&flags.runDurationStr, "run-duration", "5m", "the duration of time to run the program")
	flags.flagSet.StringVar(&flags.Config, "config", "/etc/metrictank/metrictank.ini", "configuration file path")
	flags.flagSet.IntVar(&flags.PartitionFrom, "partition-from", 0, "the partition to load the index from")
	flags.flagSet.IntVar(&flags.PartitionTo, "partition-to", -1, "load the index from all partitions up to this one (exclusive). If unset, only the partition defined with \"--partition-from\" is loaded from")
	flags.flagSet.StringVar(&flags.graceDurationStr, "grace-duration", "0s", "todo")
	flags.flagSet.StringVar(&flags.Prefix, "prefix", "", "only show metrics with a name that has this prefix")
	flags.flagSet.StringVar(&flags.Substr, "substr", "", "only show metrics with a name that has this substring")
	flags.flagSet.BoolVar(&flags.GroupByName, "group-by-name", false, "group out-of-order metrics by name")
	flags.flagSet.StringVar(&flags.GroupByTag, "group-by-tag", "", "group out-of-order metrics by the specified tag")

	return &flags
}

func (flags *Flags) Parse(args []string) {
	err := flags.flagSet.Parse(args)
	if err != nil {
		log.Fatalf("failed to parse application flags %v: %s", args, err.Error)
		os.Exit(1)
	}

	path := ""
	if _, err := os.Stat(flags.Config); err == nil {
		path = flags.Config
	}
	config, err := globalconf.NewWithOptions(&globalconf.Options{
		Filename:  path,
		EnvPrefix: "MT_",
	})
	if err != nil {
		log.Fatalf("error with configuration file: %s", err.Error())
		os.Exit(1)
	}
	_ = cassandra.ConfigSetup()
	inKafkaMdm.ConfigSetup()
	config.Parse()

	flags.RunDuration, err = time.ParseDuration(flags.runDurationStr)
	if err != nil {
		log.Fatalf("failed to parse run duration %s: %s", flags.runDurationStr, err.Error)
		os.Exit(1)
	}
	flags.GraceDuration, err = time.ParseDuration(flags.graceDurationStr)
	if err != nil {
		log.Fatalf("failed to parse grace duration %s: %s", flags.graceDurationStr, err.Error)
		os.Exit(1)
	}

	if flags.GroupByName == false && flags.GroupByTag == "" {
		log.Fatalf("must specify one of -group-by-name or -group-by-tag")
		os.Exit(1)
	}
}

func (flags *Flags) Usage() {
	fmt.Fprintln(os.Stderr, "mt-kafka-mdm-report-out-of-order")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Inspects what's flowing through kafka (in mdm format) and reports out of order data grouped by metric name or tag (does not take into account reorder buffer)")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "# Mechanism")
	fmt.Fprintln(os.Stderr, "* it sniffs points being added on a per-series (metric Id) level")
	fmt.Fprintln(os.Stderr, "* for every series, tracks the last 'correct' point.  E.g. a point that was able to be added to the series because its timestamp is higher than any previous timestamp")
	fmt.Fprintln(os.Stderr, "* if for any series, a point comes in with a timestamp equal or lower than the last point correct point - which metrictank would not add unless it falls within the reorder buffer - it triggers an event for this out-of-order point")
	fmt.Fprintln(os.Stderr, "Usage:")
	fmt.Fprintln(os.Stderr, "  mt-kafka-mdm-report-out-of-order [flags]")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "out-of-order metrics are printed in the following format based on grouping")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "# Grouped by Names")
	fmt.Fprintln(os.Stderr, "name=<name1> count=<count1>")
	fmt.Fprintln(os.Stderr, "name=<name1> count=<count1>")
	fmt.Fprintln(os.Stderr, "...")
	fmt.Fprintln(os.Stderr, "# Grouped by Tags")
	fmt.Fprintln(os.Stderr, "tag=<tag1> count=<count1>")
	fmt.Fprintln(os.Stderr, "tag=<tag2> count=<count2>")
	fmt.Fprintln(os.Stderr, "...")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintf(os.Stderr, "flags:")
	flags.flagSet.PrintDefaults()
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "EXAMPLES:")
	fmt.Fprintln(os.Stderr, "  mt-kafka-mdm-report-out-of-order -group-by-name -config metrictank.ini -partition-from 0")
	fmt.Fprintln(os.Stderr, "  mt-kafka-mdm-report-out-of-order -grace-duration 30s -group-by-tag namespace -config metrictank.ini -partition-from 0 -partition-to 3")
}

func ParseFlags() Flags {
	flags := NewFlags()

	flag.Usage = flags.Usage

	if len(os.Args) == 2 && (os.Args[1] == "-h" || os.Args[1] == "--help") {
		flag.Usage()
		os.Exit(0)
	}
	if len(os.Args) < 2 {
		flag.Usage()
		os.Exit(-1)
	}

	flags.Parse(os.Args[1:])

	return *flags
}
