package main

import (
	"strings"
	"sync"
	"time"

	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/schema/msg"
	log "github.com/sirupsen/logrus"
)

type Track struct {
	Name string
	Tags []string

	LatestTimestamp int64
}

type Tracker map[schema.MKey]Track

// find out of order metrics
type inputOOOFinder struct {
	graceDuration time.Duration
	prefix        string
	substr        string

	tracker Tracker

	groupByName   bool
	groupedByName *map[string]int
	groupByTag    string
	groupedByTag  *map[string]int

	lock sync.Mutex
}

func newInputOOOFinder(graceDuration time.Duration, prefix string, substr string, tracker Tracker, groupByName bool, groupedByName *map[string]int, groupByTag string, groupedByTag *map[string]int) *inputOOOFinder {
	return &inputOOOFinder{
		graceDuration,
		prefix,
		substr,

		tracker,

		groupByName,
		groupedByName,
		groupByTag,
		groupedByTag,

		sync.Mutex{},
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
	} else if metricTime+int64(ip.graceDuration.Seconds()) < track.LatestTimestamp {
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
		log.Errorf("metric definition for key=%v not found", mp.MKey)
		return
	}

	ip.processTrack(mp.MKey, int64(mp.Time), track)
}

func (ip *inputOOOFinder) ProcessIndexControlMsg(msg schema.ControlMsg, partition int32) {

}
