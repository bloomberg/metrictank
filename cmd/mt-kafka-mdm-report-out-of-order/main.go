package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/grafana/globalconf"
	"github.com/grafana/metrictank/idx/cassandra"
	inKafkaMdm "github.com/grafana/metrictank/input/kafkamdm"
	"github.com/grafana/metrictank/logger"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/schema/msg"
	"github.com/grafana/metrictank/stats"
	log "github.com/sirupsen/logrus"
)

var (
)

type Tracker struct {
	Head      Msg    // last successfully added message
	Bad       Msg    // current (last seen) point that could not be added (assuming no re-order buffer)
	NumBad    int    // number of failed points since last successful add
	DeltaTime uint32 // delta between Head and Bad time properties in seconds (point timestamps)
	DeltaSeen uint32 // delta between Head and Bad seen time in seconds (consumed from kafka)
	Definition schema.MetricDefinition
}

type Msg struct {
	Part int32
	Seen time.Time
	Md   schema.MetricData // either this one or below will be valid depending on input
	Mp   schema.MetricPoint
}

func (m Msg) Time() uint32 {
	if m.Md.Id != "" {
		return uint32(m.Md.Time)
	}
	return m.Mp.Time
}

// find out of order metrics
type inputOOOFinder struct {
	prefix string
	substr string
	doUnknownMP bool

	data map[schema.MKey]Tracker
	definitions map[schema.MKey]schema.MetricDefinition
	outOfOrder *[]Tracker
	lock sync.Mutex
}

func newInputOOOFinder(prefix string, substr string, doUnknownMP bool, definitions map[schema.MKey]schema.MetricDefinition, outOfOrder *[]Tracker) *inputOOOFinder {
	return &inputOOOFinder{
		prefix,
		substr,
		doUnknownMP,

		make(map[schema.MKey]Tracker),
		definitions,
		outOfOrder,
		sync.Mutex{},
	}
}

func (ip *inputOOOFinder) ProcessMetricData(metric *schema.MetricData, partition int32) {
	if ip.prefix != "" && !strings.HasPrefix(metric.Name, ip.prefix) {
		return
	}
	if ip.substr != "" && !strings.Contains(metric.Name, ip.substr) {
		return
	}
	mkey, err := schema.MKeyFromString(metric.Id)
	if err != nil {
		log.Errorf("could not parse id %q: %s", metric.Id, err.Error())
		return
	}

	now := Msg{
		Part: partition,
		Seen: time.Now(),
		Md:   *metric,
	}
	ip.lock.Lock()
	tracker, ok := ip.data[mkey]
	if !ok {
		ip.data[mkey] = Tracker{
			Head: now,
		}
	} else {
		if uint32(metric.Time) > tracker.Head.Time() {
			tracker.Head = now
			tracker.NumBad = 0
			ip.data[mkey] = tracker
		} else {
			// if metric time <= head point time, update "bad", generate event and print
			tracker.Bad = now
			tracker.NumBad += 1
			tracker.DeltaTime = tracker.Head.Time() - uint32(metric.Time)
			tracker.DeltaSeen = uint32(now.Seen.Unix()) - uint32(tracker.Head.Seen.Unix())
			ip.data[mkey] = tracker
			*(ip.outOfOrder) = append(*ip.outOfOrder, tracker)
		}
	}
	_, ok = ip.definitions[mkey]
	if !ok {
		ip.definitions[mkey] = schema.MetricDefinition{
			Id: mkey,
			Tags: metric.Tags,
		}
	}
	ip.lock.Unlock()
}

func (ip *inputOOOFinder) ProcessMetricPoint(mp schema.MetricPoint, format msg.Format, partition int32) {
	now := Msg{
		Part: partition,
		Seen: time.Now(),
		Mp:   mp,
	}
	ip.lock.Lock()
	tracker, ok := ip.data[mp.MKey]
	if !ok {
		if !ip.doUnknownMP {
			return
		}
		ip.data[mp.MKey] = Tracker{
			Head: now,
		}
	} else {
		if mp.Time > tracker.Head.Time() {
			// highest TS seen so far -> update "head"
			tracker.Head = now
			tracker.NumBad = 0
			ip.data[mp.MKey] = tracker
		} else {
			// if metric time <= head point time, update "bad", generate event and print
			tracker.Bad = now
			tracker.NumBad += 1
			tracker.DeltaTime = tracker.Head.Time() - mp.Time
			tracker.DeltaSeen = uint32(now.Seen.Unix()) - uint32(tracker.Head.Seen.Unix())

			var exists bool
			tracker.Definition, exists = ip.definitions[mp.MKey]
			if !exists {
				log.Errorf("metric definition not found")
			}

			ip.data[mp.MKey] = tracker
			*(ip.outOfOrder) = append(*ip.outOfOrder, tracker)
		}
	}
	ip.lock.Unlock()
}

func (ip *inputOOOFinder) ProcessIndexControlMsg(msg schema.ControlMsg, partition int32) {

}

func main() {
	var confFile string
	var partitionFrom, partitionTo int
	var prefix string
	var substr string
	var doUnknownMP bool
	var groupByName bool
	var groupByTag string

	globalFlags := flag.NewFlagSet("global config flags", flag.ExitOnError)
	globalFlags.StringVar(&confFile, "config", "/etc/metrictank/metrictank.ini", "configuration file path")
	globalFlags.IntVar(&partitionFrom, "partition-from", 0, "the partition to load the index from")
	globalFlags.IntVar(&partitionTo, "partition-to", -1, "load the index from all partitions up to this one (exclusive). If unset, only the partition defined with \"--partition-from\" is loaded from")
	globalFlags.StringVar(&prefix, "prefix", "", "only show metrics with a name that has this prefix")
	globalFlags.StringVar(&substr, "substr", "", "only show metrics with a name that has this substring")
	globalFlags.BoolVar(&doUnknownMP, "do-unknown-mp", true, "process MetricPoint messages for which no MetricData messages have been seen. If you use prefix/substr filter, this may report on metrics you wanted to filter out!")
	globalFlags.BoolVar(&groupByName, "group-by-name", false, "group out-of-order metrics by name")
	globalFlags.StringVar(&groupByTag, "group-by-tag", "", "group out-of-order metrics by the specified tag")
	
	cassFlags := cassandra.ConfigSetup()

	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "mt-kafka-mdm-report-out-of-order")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Inspects what's flowing through kafka (in mdm format) and reports out of order data grouped by metric name or tag (does not take into account reorder buffer)")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "# Mechanism")
		fmt.Fprintln(os.Stderr, "* it sniffs points being added on a per-series (metric Id) level")
		fmt.Fprintln(os.Stderr, "* for every series, tracks the last 'correct' point.  E.g. a point that was able to be added to the series because its timestamp is higher than any previous timestamp")
		fmt.Fprintln(os.Stderr, "* if for any series, a point comes in with a timestamp equal or lower than the last point correct point - which metrictank would not add unless it falls within the reorder buffer - it triggers an event for this out-of-order point")
		fmt.Fprintln(os.Stderr, "Usage:")
		fmt.Fprintln(os.Stderr, "  mt-kafka-mdm-report-out-of-order [global config flags] <idxtype> [idx config flags]\n")
		fmt.Fprintln(os.Stderr, "out-of-order metrics are printed in the following format based on grouping")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "# Grouped by Metric Name")
		fmt.Fprintln(os.Stderr, "<name> : <count>")
		fmt.Fprintln(os.Stderr, "# Grouped by Tag")
		fmt.Fprintln(os.Stderr, "<tag> : <count>")
		fmt.Fprintf(os.Stderr, "\nglobal config flags:\n\n")
		globalFlags.PrintDefaults()
		fmt.Fprintln(os.Stderr)
		fmt.Fprintf(os.Stderr, "idxtype: only 'cass' supported for now\n\n")
		fmt.Fprintf(os.Stderr, "cass config flags:\n\n")
		cassFlags.PrintDefaults()
		fmt.Fprintln(os.Stderr, "\nEXAMPLES:")
		fmt.Fprintln(os.Stderr, "mt-kafka-mdm-report-out-of-order -group-by-name -config metrictank.ini -partition-from 0 cass -hosts cassandra:9042")
		fmt.Fprintln(os.Stderr, "mt-kafka-mdm-report-out-of-order -group-by-tag namespace -config metrictank.ini -partition-from 0 -partition-to 3 cass -hosts cassandra:9042")
	}

	
	if len(os.Args) == 2 && (os.Args[1] == "-h" || os.Args[1] == "--help") {
		flag.Usage()
		os.Exit(0)
	}

	if len(os.Args) < 2 {
		flag.Usage()
		os.Exit(-1)
	}

	var cassI int
	for i, v := range os.Args {
		if v == "cass" {
			cassI = i
		}
	}

	if cassI == 0 {
		log.Println("only indextype 'cass' supported")
		flag.Usage()
		os.Exit(1)
	}

	err := globalFlags.Parse(os.Args[1:cassI])
	if err != nil {
		log.Fatalf("failed to parse global flags: %s", err.Error)
		os.Exit(1)
	}

	cassFlags.Parse(os.Args[cassI+1:])

	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)
	instance := "mt-kafka-mdm-sniff-out-of-order" + strconv.Itoa(rand.Int())

	// Only try and parse the conf file if it exists
	path := ""
	if _, err := os.Stat(confFile); err == nil {
		path = confFile
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

	if groupByName == true && groupByTag != "" {
		log.Fatalf("the -group-by-name and -group-by-tag flags are mutually exclusive")
	}
	if groupByName == false && groupByTag == "" {
		log.Fatalf("must specify either -group-by-name or -group-by-tag")
	}

	// todo we need to read cassandra flags too
	// todo doc
	// load only the paritions kafkamdm is reading from
	cassFlags.Parse(os.Args[cassI+1:])
	cassandra.CliConfig.Enabled = true
	//cassandra.CliConfig.Hosts = "cassandra:9042"
	cassIdx := cassandra.New(cassandra.CliConfig)
	err = cassIdx.InitBare()
	if err != nil {
		log.Fatalf("error initializing cassandra index: %s", err.Error())
		os.Exit(1)
	}
	
	// todo: cargo-culted, do I need this?
	// we don't want to filter any metric definitions during the loading
	// so MaxStale is set to 0
	/*memory.IndexRules = conf.IndexRules{
		Rules: nil,
		Default: conf.IndexRule{
			Name:     "default",
			Pattern:  regexp.MustCompile(""),
			MaxStale: 0,
		},
	}*/
	
	defs := make([]schema.MetricDefinition, 0)
	// todo: find a reasonable way to get the partitions from globalconf, adding to cass cfg, or as a flag here (yuck, since we want the kafkamdm and cassidx partitions to match)
	fmt.Println("DOM DEBUG", partitionFrom, partitionTo)
	for partition := partitionFrom; (partitionTo == -1 && partition == partitionFrom) || (partitionTo > 0 && partition < partitionTo); partition++ {
		fmt.Println("DOM DEBUG", partition)
				defs = cassIdx.LoadPartitions([]int32{int32(partition)}, defs, time.Now())
	}

	// todo doc
	// this is kind of janky because inputooofinder already maintains its own map of mkey->point
	// but beacuse this is populated from kafka, there's no way for us to "pre-load" the metrics
	// from the index into inputooffinder
	// so instead we maintain two maps, mkey->tracker for kafka consumption and mkey->definition
	// for printing
	// we update these definitions as we see new metrics
	metricDefinitions := map[schema.MKey]schema.MetricDefinition{}
	for _, def := range defs {
		metricDefinitions[def.Id] = def
	}

	// config may have had it disabled
	inKafkaMdm.Enabled = true

	inKafkaMdm.ConfigProcess(instance)

	stats.NewDevnull() // make sure metrics don't pile up without getting discarded

	outOfOrder := []Tracker{}

	mdm := inKafkaMdm.New()
	ctx, cancel := context.WithCancel(context.Background())
	mdm.Start(newInputOOOFinder(prefix, substr, doUnknownMP, metricDefinitions, &outOfOrder), cancel)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-sigChan:
		log.Infof("Received signal %q. Shutting down", sig)
	case <-ctx.Done():
		log.Info("Mdm input plugin signalled a fatal error. Shutting down")
	case <-time.After(5 * time.Second):
		log.Infof("todo ran out of time")
	}
	mdm.Stop()

	name := map[string]int{}

	group := map[string]int{}

	log.Infof("FINAL LEN %d", len(outOfOrder))
	for _, tracker := range outOfOrder {
		name[tracker.Definition.Name]++
		for _, tag := range tracker.Definition.Tags {
			kv := strings.Split(tag, "=")
			if kv[0] == groupByTag {
				group[kv[1]]++
			}
		}
	}

	for key, value := range name {
		log.Infof("%s : %d", key, value)
	}
	for key, value := range group {
		log.Infof("%s : %d", key, value)
	}
}
