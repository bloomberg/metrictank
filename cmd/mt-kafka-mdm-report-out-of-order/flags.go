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

	RunDuration    time.Duration
	Config         string
	PartitionFrom  int
	PartitionTo    int
	MetricInterval int
	ReorderWindow  int
	Prefix         string
	Substr         string
	GroupByName    bool
	GroupByTag     string
}

func NewFlags() *Flags {
	var flags Flags

	flags.flagSet = flag.NewFlagSet("application flags", flag.ExitOnError)
	flags.flagSet.DurationVar(&flags.RunDuration, "run-duration", 5*time.Minute, "the duration of time to run the program")
	flags.flagSet.StringVar(&flags.Config, "config", "/etc/metrictank/metrictank.ini", "configuration file path")
	flags.flagSet.IntVar(&flags.PartitionFrom, "partition-from", 0, "the partition to load the index from")
	flags.flagSet.IntVar(&flags.PartitionTo, "partition-to", -1, "load the index from all partitions up to this one (exclusive). If unset, only the partition defined with \"--partition-from\" is loaded from")
	flags.flagSet.IntVar(&flags.MetricInterval, "metric-interval", 30, "the metric interval in seconds")
	flags.flagSet.IntVar(&flags.ReorderWindow, "reorder-window", 0, "the size of the reorder buffer window")
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

	if flags.GroupByName == false && flags.GroupByTag == "" {
		log.Fatalf("must specify one of -group-by-name or -group-by-tag")
		os.Exit(1)
	}
}

func (flags *Flags) Usage() {
	fmt.Fprintln(os.Stderr, "mt-kafka-mdm-report-out-of-order")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Inspects what's flowing through kafka (in mdm format) and reports out of order data grouped by metric name or tag, taking into account the reorder buffer)")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "# Mechanism")
	fmt.Fprintln(os.Stderr, "* it sniffs points being added on a per-series (metric Id) level")
	fmt.Fprintln(os.Stderr, "* for every series, tracks the last 'correct' point.  E.g. a point that was able to be added to the series because its timestamp is higher than any previous timestamp")
	fmt.Fprintln(os.Stderr, "* if for any series, a point comes in with a timestamp equal or lower than the last point correct point - which metrictank would not add unless it falls within the reorder buffer - it triggers an event for this out-of-order point")
	fmt.Fprintln(os.Stderr, "* the reorder buffer is described by the metric interval and the window size")
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
	fmt.Fprintln(os.Stderr, "  mt-kafka-mdm-report-out-of-order -metric-interval 30 -reorder-window 5 -group-by-tag namespace -config metrictank.ini -partition-from 0 -partition-to 3")
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
