package main

import (
	"strings"
	"sync"
	"time"

	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/schema/msg"
	log "github.com/sirupsen/logrus"
)

type Tracker struct {
	Head      Msg    // last successfully added message
	Bad       Msg    // current (last seen) point that could not be added (assuming no re-order buffer)
	NumBad    int    // number of failed points since last successful add
	DeltaTime uint32 // delta between Head and Bad time properties in seconds (point timestamps)
	DeltaSeen uint32 // delta between Head and Bad seen time in seconds (consumed from kafka)
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
	graceDuration time.Duration
	prefix        string
	substr        string
	doUnknownMP   bool

	data        map[schema.MKey]Tracker
	definitions map[schema.MKey]schema.MetricDefinition

	groupByTag    string
	groupedByName *map[string]int
	groupedByTag  *map[string]int

	lock sync.Mutex
}

func newInputOOOFinder(graceDuration time.Duration, prefix string, substr string, doUnknownMP bool, definitions map[schema.MKey]schema.MetricDefinition, groupByTag string, groupedByName *map[string]int, groupedByTag *map[string]int) *inputOOOFinder {
	return &inputOOOFinder{
		graceDuration,
		prefix,
		substr,
		doUnknownMP,

		make(map[schema.MKey]Tracker),
		definitions,

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

	// update index
	_, ok := ip.definitions[mkey]
	if !ok {
		ip.definitions[mkey] = schema.MetricDefinition{
			Name: metric.Name,
			Tags: metric.Tags,
		}
	}

	now := Msg{
		Part: partition,
		Seen: time.Now(),
		Md:   *metric,
	}
	tracker, ok := ip.data[mkey]
	if !ok {
		ip.data[mkey] = Tracker{
			Head: now,
		}
	} else {
		if uint32(metric.Time)+uint32(ip.graceDuration.Seconds()) > tracker.Head.Time() {
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
			(*ip.groupedByName)[metric.Name]++
			for _, tag := range metric.Tags {
				kv := strings.Split(tag, "=")
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

	metricDefinition, exists := ip.definitions[mp.MKey]
	if !exists {
		log.Errorf("metric definition not found")
		return
	}

	if ip.prefix != "" && !strings.HasPrefix(metricDefinition.Name, ip.prefix) {
		return
	}
	if ip.substr != "" && !strings.Contains(metricDefinition.Name, ip.substr) {
		return
	}

	now := Msg{
		Part: partition,
		Seen: time.Now(),
		Mp:   mp,
	}
	tracker, ok := ip.data[mp.MKey]
	if !ok {
		if !ip.doUnknownMP {
			return
		}
		ip.data[mp.MKey] = Tracker{
			Head: now,
		}
	} else {
		if mp.Time+uint32(ip.graceDuration.Seconds()) > tracker.Head.Time() {
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

			ip.data[mp.MKey] = tracker
			(*ip.groupedByName)[metricDefinition.Name]++
			for _, tag := range metricDefinition.Tags {
				kv := strings.Split(tag, "=")
				if kv[0] == ip.groupByTag {
					(*ip.groupedByTag)[kv[1]]++
				}
			}
		}
	}
}

func (ip *inputOOOFinder) ProcessIndexControlMsg(msg schema.ControlMsg, partition int32) {

}
