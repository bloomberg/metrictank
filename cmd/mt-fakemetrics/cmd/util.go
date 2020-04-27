// Copyright © 2018 Grafana Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/grafana/metrictank/cmd/mt-fakemetrics/out"
	"github.com/grafana/metrictank/cmd/mt-fakemetrics/out/carbon"
	"github.com/grafana/metrictank/cmd/mt-fakemetrics/out/gnet"
	"github.com/grafana/metrictank/cmd/mt-fakemetrics/out/kafkamdm"
	"github.com/grafana/metrictank/cmd/mt-fakemetrics/out/stdout"
)

func checkOutputs() {
	if carbonAddr == "" && gnetAddr == "" && kafkaMdmAddr == "" && !stdoutOut {
		log.Fatal(4, "must use at least either carbon, gnet, kafka-mdm or stdout")
	}
}

func getOutput() out.Out {
	var outs []out.Out

	if carbonAddr != "" {
		if orgs > 1 {
			log.Fatal(4, "can only simulate 1 org when using carbon output")
		}
		o, err := carbon.New(carbonAddr, stats)
		if err != nil {
			log.Fatal(4, "failed to create carbon output. %s", err)
		}
		outs = append(outs, o)
	}

	if gnetAddr != "" {
		if orgs > 1 {
			log.Fatal(4, "can only simulate 1 org when using gnet output")
		}
		if gnetKey == "" {
			log.Fatal(4, "to use gnet, a key must be specified")
		}
		o, err := gnet.New(gnetAddr, gnetKey, stats)
		if err != nil {
			log.Fatal(4, "failed to create gnet output. %s", err)
		}
		outs = append(outs, o)
	}

	if kafkaMdmAddr != "" {
		if kafkaMdmTopic == "" {
			log.Fatal(4, "kafka-mdm needs the topic to be set")
		}
		o, err := kafkamdm.New(kafkaMdmTopic, []string{kafkaMdmAddr}, kafkaCompression, 30*time.Second, stats, partitionScheme, kafkaMdmV2)
		if err != nil {
			log.Fatal(4, "failed to create kafka-mdm output. %s", err)
		}
		outs = append(outs, o)
	}

	if stdoutOut {
		outs = append(outs, stdout.New(stats))
	}
	if len(outs) == 0 {
		log.Fatal("need to define an output")
	}
	o := outs[0]
	if len(outs) > 1 {
		o = out.NewFanOut(outs)
	}
	for i := len(filterStrings) - 1; i >= 0; i-- {
		filterString := filterStrings[i]
		// <name>:<opts>
		// in the future, <opts> will probably be multiple key=val pairs
		splits := strings.SplitN(filterString, ":", 2)
		switch splits[0] {
		case "offset":
			if len(splits) != 2 {
				log.Fatal("offset option must be specified for offset filter")
			}
			var err error
			o, err = out.NewOffsetFilter(o, splits[1])
			if err != nil {
				log.Fatal(err.Error())
			}
		default:
			log.Fatalf("unrecognized filter %q", splits[0])
		}
	}
	return o

}
