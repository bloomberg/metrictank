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

	Latest int64
}

type Tracker map[schema.MKey]Track

// find out of order metrics
type inputOOOFinder struct {
	graceDuration time.Duration
	prefix        string
	substr        string
	doUnknownMP   bool

	tracker Tracker

	groupByTag    string
	groupedByName *map[string]int
	groupedByTag  *map[string]int

	lock sync.Mutex
}

func newInputOOOFinder(graceDuration time.Duration, prefix string, substr string, doUnknownMP bool, tracker Tracker, groupByTag string, groupedByName *map[string]int, groupedByTag *map[string]int) *inputOOOFinder {
	return &inputOOOFinder{
		graceDuration,
		prefix,
		substr,
		doUnknownMP,

		tracker,

		groupByTag,
		groupedByName,
		groupedByTag,

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

	ip.lock.Lock()
	defer ip.lock.Unlock()

	track, exists := ip.tracker[mkey]
	if !exists {
		ip.tracker[mkey] = Track{
			Name:   metric.Name,
			Tags:   metric.Tags,
			Latest: metric.Time,
		}
	} else {
		if metric.Time > track.Latest {
			track.Latest = metric.Time
			ip.tracker[mkey] = track
		} else if metric.Time+int64(ip.graceDuration.Seconds()) <= track.Latest {
			// increment grouping counts
			(*ip.groupedByName)[metric.Name]++
			for _, tag := range metric.Tags {
				kv := strings.Split(tag, "=")
				if len(kv) != 2 {
					log.Errorf("unexpected tag encoding %s", tag)
					continue
				}
				if kv[0] == ip.groupByTag {
					(*ip.groupedByTag)[kv[1]]++
				}
			}
		}
	}
}

func (ip *inputOOOFinder) ProcessMetricPoint(mp schema.MetricPoint, format msg.Format, partition int32) {
	ip.lock.Lock()
	defer ip.lock.Unlock()

	track, exists := ip.tracker[mp.MKey]
	if !exists {
		log.Errorf("metric definition not found")
		return
	}

	if ip.prefix != "" && !strings.HasPrefix(track.Name, ip.prefix) {
		return
	}
	if ip.substr != "" && !strings.Contains(track.Name, ip.substr) {
		return
	}

	if int64(mp.Time) > track.Latest {
		track.Latest = int64(mp.Time)
		ip.tracker[mp.MKey] = track
	} else if int64(mp.Time)+int64(ip.graceDuration.Seconds()) > track.Latest {
		// increment grouping counts
		(*ip.groupedByName)[track.Name]++
		for _, tag := range track.Tags {
			kv := strings.Split(tag, "=")
			if kv[0] == ip.groupByTag {
				(*ip.groupedByTag)[kv[1]]++
			}
		}
	}
}

func (ip *inputOOOFinder) ProcessIndexControlMsg(msg schema.ControlMsg, partition int32) {

}
