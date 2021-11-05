package main

import (
	"os"
	"strings"
	"sync"
	"time"

	"github.com/grafana/metrictank/idx/cassandra"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/schema/msg"
	log "github.com/sirupsen/logrus"
)

type Track struct {
	Name     string
	Interval int
	Tags     []string

	LatestTimestamp int64
}

type Tracker map[schema.MKey]Track

// find out of order metrics
type inputOOOFinder struct {
	prefix string
	substr string

	tracker       Tracker
	reorderWindow int

	groupByName   bool
	groupedByName *map[string]int
	groupByTag    string
	groupedByTag  *map[string]int

	lock sync.Mutex
}

func newInputOOOFinder(prefix string, substr string, partitionFrom int, partitionTo int, reorderWindow int, groupByName bool, groupedByName *map[string]int, groupByTag string, groupedByTag *map[string]int) *inputOOOFinder {
	cassandraIndex := cassandra.New(cassandra.CliConfig)
	err := cassandraIndex.InitBare()
	if err != nil {
		log.Fatalf("error initializing cassandra index: %s", err.Error())
		os.Exit(1)
	}

	metricDefinitions := make([]schema.MetricDefinition, 0)
	for partition := partitionFrom; (partitionTo == -1 && partition == partitionFrom) || (partitionTo > 0 && partition < partitionTo); partition++ {
		metricDefinitions = cassandraIndex.LoadPartitions([]int32{int32(partition)}, metricDefinitions, time.Now())
	}

	tracker := Tracker{}
	for _, metricDefinition := range metricDefinitions {
		tracker[metricDefinition.Id] = Track{
			Name:     metricDefinition.Name,
			Interval: metricDefinition.Interval,
			Tags:     metricDefinition.Tags,
		}
	}

	return &inputOOOFinder{
		prefix: prefix,
		substr: substr,

		tracker:       tracker,
		reorderWindow: reorderWindow,

		groupByName:   groupByName,
		groupedByName: groupedByName,
		groupByTag:    groupByTag,
		groupedByTag:  groupedByTag,

		lock: sync.Mutex{},
	}
}

func (ip *inputOOOFinder) processTrack(metricKey schema.MKey, metricTime int64, track Track) {
	if ip.prefix != "" && !strings.HasPrefix(track.Name, ip.prefix) {
		return
	}
	if ip.substr != "" && !strings.Contains(track.Name, ip.substr) {
		return
	}

	if metricTime > track.LatestTimestamp {
		track.LatestTimestamp = metricTime
		ip.tracker[metricKey] = track
	} else if metricTime+int64(track.Interval*ip.reorderWindow) < track.LatestTimestamp {
		// increment grouping counts
		if ip.groupByName == true {
			(*ip.groupedByName)[track.Name]++
		}

		if ip.groupByTag != "" {
			for _, tag := range track.Tags {
				kv := strings.Split(tag, "=")
				if len(kv) != 2 {
					log.Errorf("unexpected tag encoding tag=%q", tag)
					continue
				}
				if kv[0] == ip.groupByTag {
					(*ip.groupedByTag)[kv[1]]++
				}
			}
		}
	}
}

func (ip *inputOOOFinder) ProcessMetricData(metric *schema.MetricData, partition int32) {
	metricKey, err := schema.MKeyFromString(metric.Id)
	if err != nil {
		log.Errorf("failed to get metric key from id=%v: %s", metric.Id, err.Error())
		return
	}

	ip.lock.Lock()
	defer ip.lock.Unlock()

	track, exists := ip.tracker[metricKey]
	if !exists {
		ip.tracker[metricKey] = Track{
			Name:            metric.Name,
			Tags:            metric.Tags,
			LatestTimestamp: metric.Time,
		}
		return
	}

	ip.processTrack(metricKey, metric.Time, track)
}

func (ip *inputOOOFinder) ProcessMetricPoint(mp schema.MetricPoint, format msg.Format, partition int32) {
	ip.lock.Lock()
	defer ip.lock.Unlock()

	track, exists := ip.tracker[mp.MKey]
	if !exists {
		log.Errorf("metric definition for key=%v in partition=%d not found", mp.MKey, partition)
		return
	}

	ip.processTrack(mp.MKey, int64(mp.Time), track)
}

func (ip *inputOOOFinder) ProcessIndexControlMsg(msg schema.ControlMsg, partition int32) {

}
