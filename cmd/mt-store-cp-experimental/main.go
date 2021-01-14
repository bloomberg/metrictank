package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/logger"
	"github.com/grafana/metrictank/store/cassandra"
	hostpool "github.com/hailocab/go-hostpool"
	log "github.com/sirupsen/logrus"
)

const minToken = math.MinInt64
const maxToken = math.MaxInt64 // 9223372036854775807

var (
	sourceCassandraAddrs         = flag.String("source-cassandra-addrs", "localhost", "cassandra host (may be given multiple times as comma-separated list)")
	destCassandraAddrs           = flag.String("dest-cassandra-addrs", "localhost", "cassandra host (may be given multiple times as comma-separated list)")
	cassandraKeyspace            = flag.String("cassandra-keyspace", "metrictank", "cassandra keyspace to use for storing the metric data table")
	cassandraConsistency         = flag.String("cassandra-consistency", "one", "write consistency (any|one|two|three|quorum|all|local_quorum|each_quorum|local_one")
	cassandraHostSelectionPolicy = flag.String("cassandra-host-selection-policy", "tokenaware,hostpool-epsilon-greedy", "")
	cassandraTimeout             = flag.String("cassandra-timeout", "1s", "cassandra timeout")
	cassandraConcurrency         = flag.Int("cassandra-concurrency", 20, "max number of concurrent reads to cassandra.")
	cassandraRetries             = flag.Int("cassandra-retries", 0, "how many times to retry a query before failing it")
	cassandraDisableHostLookup   = flag.Bool("cassandra-disable-host-lookup", false, "disable host lookup (useful if going through proxy)")
	cqlProtocolVersion           = flag.Int("cql-protocol-version", 4, "cql protocol version to use")

	cassandraSSL              = flag.Bool("cassandra-ssl", false, "enable SSL connection to cassandra")
	cassandraCaPath           = flag.String("cassandra-ca-path", "/etc/metrictank/ca.pem", "cassandra CA certificate path when using SSL")
	cassandraHostVerification = flag.Bool("cassandra-host-verification", true, "host (hostname and server cert) verification when using SSL")

	cassandraAuth     = flag.Bool("cassandra-auth", false, "enable cassandra authentication")
	cassandraUsername = flag.String("cassandra-username", "cassandra", "username for authentication")
	cassandraPassword = flag.String("cassandra-password", "cassandra", "password for authentication")

	startTs      = flag.Int("start-timestamp", 0, "timestamp at which to start, defaults to 0")
	endTs        = flag.Int("end-timestamp", math.MaxInt32, "timestamp at which to stop, defaults to int max")
	startToken   = flag.Int64("start-token", minToken, "token to start at (inclusive), defaults to math.MinInt64")
	endToken     = flag.Int64("end-token", maxToken, "token to stop at (inclusive), defaults to math.MaxInt64")
	numThreads   = flag.Int("threads", 1, "number of workers to use to process data")
	maxBatchSize = flag.Int("max-batch-size", 10, "max number of queries per batch")
	ttlAdjust    = flag.Int("ttl-adjust", 0, "seconds to add to TTL (can be negative for lower TTL)")

	idxTable       = flag.String("idx-table", "metric_idx", "idx table in cassandra")
	partitionFrom  = flag.Int("partition-from", 0, "process ids for these partitions (comma separated list of partition numbers or '*' for all)")
	numPartitions  = flag.Int("partitions", 1, "process ids for these partitions (comma separated list of partition numbers or '*' for all)")
	aggsFile       = flag.String("agg-file", "/etc/metrictank/storage-aggregation.conf", "Path to aggregation file")
	hasTagValue    = flag.String("has-tag-value", "", "If specified, only include data with tag=value")
	hasNoTag       = flag.String("has-no-tag", "", "If specified, only include data without specified tag key")
	rollupInterval = flag.Int("rollup-interval", 3600, "If greater than 0, the interval of the data to copy")

	progressRows = flag.Int("progress-rows", 1000000, "number of rows between progress output")

	verbose = flag.Bool("verbose", false, "show every record being processed")

	timeStarted time.Time
	doneKeys    uint64
	doneRows    uint64
)

func init() {
	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)
}

func main() {
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "mt-store-cp-experimental [flags] table-in table-out")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Copies data in Cassandra to use another table (and possibly another cluster).")
		fmt.Fprintln(os.Stderr, "It is up to you to assure table-out exists before running this tool")
		fmt.Fprintln(os.Stderr, "This tool is EXPERIMENTAL and needs doublechecking whether data is successfully written to Cassandra")
		fmt.Fprintln(os.Stderr, "see https://github.com/grafana/metrictank/pull/909 for details")
		fmt.Fprintln(os.Stderr, "Please report good or bad experiences in the above ticket or in a new one")
		fmt.Println("Flags:")
		flag.PrintDefaults()
		os.Exit(-1)
	}
	flag.Parse()

	if flag.NArg() != 2 {
		log.Printf("Expected 2 positional args (table in and table out): got %d", flag.NArg())
		flag.Usage()
		os.Exit(2)
	}

	tableIn, tableOut := flag.Arg(0), flag.Arg(1)

	if sourceCassandraAddrs == destCassandraAddrs && tableIn == tableOut {
		panic("Source and destination cannot be the same")
	}

	sourceSession, err := NewCassandraStore(sourceCassandraAddrs)

	if err != nil {
		panic(fmt.Sprintf("Failed to instantiate source cassandra: %s", err))
	}

	destSession, err := NewCassandraStore(destCassandraAddrs)

	if err != nil {
		panic(fmt.Sprintf("Failed to instantiate dest cassandra: %s", err))
	}

	update(sourceSession, destSession, tableIn, tableOut)
}

func NewCassandraStore(cassandraAddrs *string) (*gocql.Session, error) {
	cluster := gocql.NewCluster(strings.Split(*cassandraAddrs, ",")...)
	if *cassandraSSL {
		cluster.SslOpts = &gocql.SslOptions{
			CaPath:                 *cassandraCaPath,
			EnableHostVerification: *cassandraHostVerification,
		}
	}
	if *cassandraAuth {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: *cassandraUsername,
			Password: *cassandraPassword,
		}
	}
	cluster.DisableInitialHostLookup = *cassandraDisableHostLookup
	cluster.Consistency = gocql.ParseConsistency(*cassandraConsistency)
	cluster.Timeout = cassandra.ConvertTimeout(*cassandraTimeout, time.Millisecond)
	cluster.NumConns = *cassandraConcurrency
	cluster.ProtoVersion = *cqlProtocolVersion
	cluster.Keyspace = *cassandraKeyspace
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: *cassandraRetries}

	switch *cassandraHostSelectionPolicy {
	case "roundrobin":
		cluster.PoolConfig.HostSelectionPolicy = gocql.RoundRobinHostPolicy()
	case "hostpool-simple":
		cluster.PoolConfig.HostSelectionPolicy = gocql.HostPoolHostPolicy(hostpool.New(nil))
	case "hostpool-epsilon-greedy":
		cluster.PoolConfig.HostSelectionPolicy = gocql.HostPoolHostPolicy(
			hostpool.NewEpsilonGreedy(nil, 0, &hostpool.LinearEpsilonValueCalculator{}),
		)
	case "tokenaware,roundrobin":
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(
			gocql.RoundRobinHostPolicy(),
		)
	case "tokenaware,hostpool-simple":
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(
			gocql.HostPoolHostPolicy(hostpool.New(nil)),
		)
	case "tokenaware,hostpool-epsilon-greedy":
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(
			gocql.HostPoolHostPolicy(
				hostpool.NewEpsilonGreedy(nil, 0, &hostpool.LinearEpsilonValueCalculator{}),
			),
		)
	default:
		return nil, fmt.Errorf("unknown HostSelectionPolicy '%q'", *cassandraHostSelectionPolicy)
	}

	return cluster.CreateSession()
}

func shouldProcessKey(id string, tags []string) bool {
	if len(*hasNoTag) > 0 {
		for _, tag := range tags {
			if strings.HasPrefix(tag, *hasNoTag) {
				return false
			}
		}
	}
	if len(*hasTagValue) > 0 {
		for _, tag := range tags {
			if tag == *hasTagValue {
				return false
			}
		}
	}
	return true
}

func publishBatchUntilSuccess(destSession *gocql.Session, batch *gocql.Batch) *gocql.Batch {
	if batch.Size() == 0 {
		return batch
	}

	for {
		err := destSession.ExecuteBatch(batch)
		if err == nil {
			break
		}
		fmt.Fprintf(os.Stderr, "ERROR: failed to publish batch, trying again. error = %q\n", err)
	}

	return destSession.NewBatch(gocql.UnloggedBatch)
}

func completenessEstimate(partition int, lastId string) float64 {
	// get % of the way through partition using id
	maxVal := 16777215 // ffffff
	decVal, _ := strconv.ParseInt(lastId[2:8], 16, 64)

	// Percentage of the way through partitions
	partitionsDone := float64(partition-*partitionFrom) + float64(decVal)/float64(maxVal)
	return partitionsDone / float64(*numPartitions)
}

func roundToSeconds(d time.Duration) time.Duration {
	return d - (d % time.Second)
}

func printProgress(partition int, id string) {
	doneKeysSnap := atomic.LoadUint64(&doneKeys)
	doneRowsSnap := atomic.LoadUint64(&doneRows)
	completeness := completenessEstimate(partition, id)
	timeElapsed := time.Since(timeStarted)

	// Scale up to scale down to avoid fractional
	ratioLeft := (1 - completeness) / completeness
	timeRemaining := time.Duration(float64(timeElapsed) * ratioLeft)
	rowsPerSec := doneRowsSnap / (uint64(1) + uint64(timeElapsed/time.Second))
	log.Printf("WORKING: processed %d keys, %d rows, lastId = %s, %.1f%% complete, elapsed=%v, remaining=%v, rows/s=%d",
		doneKeysSnap, doneRowsSnap, id, completeness*100, roundToSeconds(timeElapsed), roundToSeconds(timeRemaining), rowsPerSec)
}

func worker(id int, jobs <-chan string, wg *sync.WaitGroup, sourceSession, destSession *gocql.Session, startTime, endTime int, tableIn, tableOut string) {
	defer wg.Done()
	var ttl int64
	var ts int
	var data []byte
	var query string

	// Since we are operating on a single key at a time, all data should live in the same partition.
	// This means batch inserts will reduce round trips without falling into the trap described here:
	// https://docs.datastax.com/en/cql/3.1/cql/cql_using/useBatch.html
	batch := destSession.NewBatch(gocql.UnloggedBatch)

	selectQuery := fmt.Sprintf("SELECT ts, data, TTL(data) FROM %s where key=? AND ts>=? AND ts<?", tableIn)
	insertQuery := fmt.Sprintf("INSERT INTO %s (data, key, ts) values(?,?,?) USING TTL ? AND TIMESTAMP ?", tableOut)

	for key := range jobs {
		rowsHandledLocally := uint64(0)
		iter := sourceSession.Query(selectQuery, key, startTime, endTime).Iter()
		if *verbose {
			log.Printf("id=%d processing rownum=%d table=%q key=%q\n", id, atomic.LoadUint64(&doneRows)+1, tableIn, key)
		}
		for iter.Scan(&ts, &data, &ttl) {

			if *verbose {
				log.Printf("id=%d processing rownum=%d table=%q key=%q ts=%d query=%q data='%x'\n", id, atomic.LoadUint64(&doneRows)+1, tableIn, key, ts, query, data)
			}
			// As 'data' is re-used for each scan, we need to make a copy of the []byte slice before assigning it to a new batch.
			safeData := make([]byte, len(data))
			copy(safeData, data)
			batch.Query(insertQuery, safeData, key, ts, ttl+int64(*ttlAdjust), ts)

			if batch.Size() >= *maxBatchSize {
				if *verbose {
					log.Printf("id=%d sending batch size=%d for key=%q ts=%d'\n", id, batch.Size(), key, ts)
				}
				batch = publishBatchUntilSuccess(destSession, batch)
			}

			rowsHandledLocally++
		}

		batch = publishBatchUntilSuccess(destSession, batch)

		if *verbose {
			log.Printf("id=%d completed table=%q key=%q\n", id, tableIn, key)
		}

		atomic.AddUint64(&doneRows, rowsHandledLocally)
		atomic.AddUint64(&doneKeys, 1)

		err := iter.Close()
		if err != nil {
			doneKeysSnap := atomic.LoadUint64(&doneKeys)
			doneRowsSnap := atomic.LoadUint64(&doneRows)
			fmt.Fprintf(os.Stderr, "ERROR: id=%d failed querying %s: %q. processed %d keys, %d rows\n", id, tableIn, err, doneKeysSnap, doneRowsSnap)
		}
	}
}

func getAggStrs(aggs []conf.Method) []string {
	types := make(map[string]struct{})

	for _, a := range aggs {
		switch a {
		case conf.Avg:
			types["cnt"] = struct{}{}
			types["sum"] = struct{}{}
		case conf.Sum:
			types["sum"] = struct{}{}
		case conf.Lst:
			types["lst"] = struct{}{}
		case conf.Max:
			types["max"] = struct{}{}
		case conf.Min:
			types["min"] = struct{}{}
		}
	}

	var ret []string
	for k := range types {
		ret = append(ret, k)
	}

	return ret
}

func update(sourceSession, destSession *gocql.Session, tableIn, tableOut string) {
	// Kick off our threads
	jobs := make(chan string, 10000)

	var wg sync.WaitGroup
	wg.Add(*numThreads)
	for i := 0; i < *numThreads; i++ {
		go worker(i, jobs, &wg, sourceSession, destSession, *startTs, *endTs, tableIn, tableOut)
	}

	timeStarted = time.Now()

	// Get the unix epoch months that are valid for this run
	startMonth := *startTs / 28 / 24 / 60 / 60
	endMonth := (*endTs - 1) / 28 / 24 / 60 / 60

	aggSchema, err := conf.ReadAggregations(*aggsFile)
	if err != nil {
		log.Fatalf("can't read aggregations file %q: %s", aggsFile, err.Error())
	}

	var months []string
	for i := startMonth; i <= endMonth; i++ {
		months = append(months, strconv.Itoa(i))
	}

	var doneRowsOld uint64

	// Key grab retry loop
	for p := *partitionFrom; p < *partitionFrom+*numPartitions; p++ {
		log.Printf("Processing partition %d", p)
		lastId := "0"
		for {
			keyItr := sourceSession.Query(fmt.Sprintf("SELECT id, name, tags FROM %s where partition=%d AND id > '%s'", *idxTable, p, lastId)).Iter()

			var name string
			var tags []string
			for keyItr.Scan(&lastId, &name, &tags) {
				if shouldProcessKey(lastId, tags) {
					_, aggs := aggSchema.Match(name)
					for _, aggStr := range getAggStrs(aggs.AggregationMethod) {
						for _, month := range months {
							key := fmt.Sprintf("%s_%s_%d_%s", lastId, aggStr, *rollupInterval, month)
							if *verbose {
								log.Printf("Genned key=%s", key)
							}
							jobs <- key
						}
					}
				}

				doneRowsSnap := atomic.LoadUint64(&doneRows)
				if doneRowsSnap-doneRowsOld > uint64(*progressRows) {
					doneRowsOld = doneRowsSnap
					printProgress(p, lastId)
				}

			}

			err := keyItr.Close()
			if err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: failed querying %s and lastid=%s: %q. processed %d keys, %d rows\n", tableIn, lastId, err, doneKeys, doneRows)
			} else {
				break
			}
		}
	}

	close(jobs)

	wg.Wait()
	log.Printf("DONE.  Processed %d keys, %d rows\n", doneKeys, doneRows)
}
