package main

import (
	"os"
	"strings"
	"sync"
	"time"

	"github.com/grafana/metrictank/idx/cassandra"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/mdata/errors"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/schema/msg"
	log "github.com/sirupsen/logrus"
)

type Track struct {
	Name string
	Tags []string

	reorderBuffer *mdata.ReorderBuffer
}

type Tracker map[schema.MKey]Track

// find out-of-order and duplicate metrics
type inputOOOFinder struct {
	prefix string
	substr string

	tracker Tracker

	groupByName             bool
	outOfOrderGroupedByName *map[string]int
	duplicatesGroupedByName *map[string]int
	groupByTag              string
	outOfOrderGroupedByTag  *map[string]int
	duplicatesGroupedByTag  *map[string]int

	lock sync.Mutex
}

func newInputOOOFinder(prefix string, substr string, partitionFrom int, partitionTo int, reorderWindow uint32, groupByName bool, outOfOrderGroupedByName *map[string]int, duplicatesGroupedByName *map[string]int, groupByTag string, outOfOrderGroupedByTag *map[string]int, duplicatesGroupedByTag *map[string]int) *inputOOOFinder {
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
			Name:          metricDefinition.Name,
			Tags:          metricDefinition.Tags,
			reorderBuffer: mdata.NewReorderBuffer(reorderWindow, uint32(metricDefinition.Interval), false),
		}
	}

	return &inputOOOFinder{
		prefix: prefix,
		substr: substr,

		tracker: tracker,

		groupByName:             groupByName,
		outOfOrderGroupedByName: outOfOrderGroupedByName,
		duplicatesGroupedByName: duplicatesGroupedByName,
		groupByTag:              groupByTag,
		outOfOrderGroupedByTag:  outOfOrderGroupedByTag,
		duplicatesGroupedByTag:  duplicatesGroupedByTag,

		lock: sync.Mutex{},
	}
}

func (ip *inputOOOFinder) incrementGroupings(groupedByName *map[string]int, groupedByTag *map[string]int, track Track) {
	if ip.groupByName == true {
		(*groupedByName)[track.Name]++
	}

	if ip.groupByTag != "" {
		for _, tag := range track.Tags {
			kv := strings.Split(tag, "=")
			if len(kv) != 2 {
				log.Errorf("unexpected tag encoding tag=%q", tag)
				continue
			}
			if kv[0] == ip.groupByTag {
				(*groupedByTag)[kv[1]]++
			}
		}
	}
}

func (ip *inputOOOFinder) processTrack(metricKey schema.MKey, metricTime int64, track Track, partition int32) {
	if ip.prefix != "" && !strings.HasPrefix(track.Name, ip.prefix) {
		return
	}
	if ip.substr != "" && !strings.Contains(track.Name, ip.substr) {
		return
	}

	_, err := track.reorderBuffer.Add(uint32(metricTime), 0) // ignore value
	if err == errors.ErrMetricTooOld {
		ip.incrementGroupings(ip.outOfOrderGroupedByName, ip.outOfOrderGroupedByTag, track)
	} else if err == errors.ErrMetricNewValueForTimestamp {
		ip.incrementGroupings(ip.duplicatesGroupedByName, ip.duplicatesGroupedByTag, track)
	} else if err != nil {
		log.Errorf("failed to add metric with name=%q and timestamp=%d from partition=%d to reorder buffer: %s", track.Name, metricTime, partition, err)
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
			Name: metric.Name,
			Tags: metric.Tags,
		}
		return
	}

	ip.processTrack(metricKey, metric.Time, track, partition)
}

func (ip *inputOOOFinder) ProcessMetricPoint(mp schema.MetricPoint, format msg.Format, partition int32) {
	ip.lock.Lock()
	defer ip.lock.Unlock()

	track, exists := ip.tracker[mp.MKey]
	if !exists {
		log.Errorf("track for metric with key=%v from partition=%d not found", mp.MKey, partition)
		return
	}

	ip.processTrack(mp.MKey, int64(mp.Time), track, partition)
}

func (ip *inputOOOFinder) ProcessIndexControlMsg(msg schema.ControlMsg, partition int32) {

}
