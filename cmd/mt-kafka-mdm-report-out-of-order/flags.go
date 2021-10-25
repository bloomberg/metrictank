package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/grafana/metrictank/idx/cassandra"
	log "github.com/sirupsen/logrus"
)

func Usage(applicationFlagSet *flag.FlagSet, cassandraFlagSet *flag.FlagSet) {
	fmt.Fprintln(os.Stderr, "mt-kafka-mdm-report-out-of-order")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Inspects what's flowing through kafka (in mdm format) and reports out of order data grouped by metric name or tag (does not take into account reorder buffer)")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "# Mechanism")
	fmt.Fprintln(os.Stderr, "* it sniffs points being added on a per-series (metric Id) level")
	fmt.Fprintln(os.Stderr, "* for every series, tracks the last 'correct' point.  E.g. a point that was able to be added to the series because its timestamp is higher than any previous timestamp")
	fmt.Fprintln(os.Stderr, "* if for any series, a point comes in with a timestamp equal or lower than the last point correct point - which metrictank would not add unless it falls within the reorder buffer - it triggers an event for this out-of-order point")
	fmt.Fprintln(os.Stderr, "Usage:")
	fmt.Fprintln(os.Stderr, "  mt-kafka-mdm-report-out-of-order [application flags] <idxtype> [idx config flags]\n")
	fmt.Fprintln(os.Stderr, "out-of-order metrics are printed in the following format based on grouping")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "# Grouped by Metric Name")
	fmt.Fprintln(os.Stderr, "<name> : <count>")
	fmt.Fprintln(os.Stderr, "# Grouped by Tag")
	fmt.Fprintln(os.Stderr, "<tag> : <count>")
	fmt.Fprintf(os.Stderr, "\napplication flags:\n\n")
	applicationFlagSet.PrintDefaults()
	fmt.Fprintln(os.Stderr)
	fmt.Fprintf(os.Stderr, "idxtype: only 'cass' supported for now\n\n")
	fmt.Fprintf(os.Stderr, "cass config flags:\n\n")
	cassandraFlagSet.PrintDefaults()
	fmt.Fprintln(os.Stderr, "\nEXAMPLES:")
	fmt.Fprintln(os.Stderr, "mt-kafka-mdm-report-out-of-order -group-by-name -config metrictank.ini -partition-from 0 cass -hosts cassandra:9042")
	fmt.Fprintln(os.Stderr, "mt-kafka-mdm-report-out-of-order -grace-duration 30s -group-by-tag namespace -config metrictank.ini -partition-from 0 -partition-to 3 cass -hosts cassandra:9042")
}

func ParseFlags() ApplicationFlags {
	applicationFlags := NewApplicationFlags()
	cassandraFlagSet := cassandra.ConfigSetup()

	flag.Usage = func() { Usage(applicationFlags.flagSet, cassandraFlagSet) }

	if len(os.Args) == 2 && (os.Args[1] == "-h" || os.Args[1] == "--help") {
		flag.Usage()
		os.Exit(0)
	}
	if len(os.Args) < 2 {
		flag.Usage()
		os.Exit(-1)
	}

	var indexTypeArgumentIndex int
	for i, v := range os.Args {
		if v == "cass" {
			indexTypeArgumentIndex = i
		}
	}
	if indexTypeArgumentIndex == 0 {
		log.Println("only indextype 'cass' supported")
		flag.Usage()
		os.Exit(1)
	}

	applicationFlags.Parse(os.Args[1:indexTypeArgumentIndex])
	cassandraFlagSet.Parse(os.Args[indexTypeArgumentIndex+1:])

	return *applicationFlags
}
