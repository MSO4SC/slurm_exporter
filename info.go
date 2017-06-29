// Copyright (c) 2017 MSO4SC - javier.carnero@atos.net
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Info Slurm collector

package main

import (
	"bytes"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
)

const (
	infoCommand   = "sinfo -h -o \"%20P %.5a %.15F\""
	iSTATESNUMBER = 3
)

var iSTATESNAMES = [3]string{"allocated", "idle", "other"}

const (
	iPARTITION = iota
	iAVAIL     = iota
	iSTATES    = iota
	iFIELDS    = iota
)

func sinfoLineParser(line string) []string {
	fields := strings.Fields(line)

	if len(fields) != iFIELDS {
		log.Warnf("sinfo line parse failed (%s): %d fields expected, %d parsed", line, iFIELDS, len(fields))
		return nil
	}

	return fields
}

// InfoCollector collects metrics from Slurm info
type InfoCollector struct {
	partitionNodes *prometheus.Desc
	slurmCommon    *SlurmCollector
}

// NewInfoCollector creates a new Slurm Info collector
func NewInfoCollector(host, sshUser, sshPass, timeZone string) *InfoCollector {
	newInfoCollector := &InfoCollector{
		partitionNodes: prometheus.NewDesc(
			"partition_nodes",
			"Nodes of the partition",
			[]string{"partition", "availability", "state"},
			nil,
		),
	}
	newSlurmCommon, err := NewSlurmCollector(host, sshUser, sshPass, timeZone)
	if err != nil {
		log.Fatalln(err.Error())
	}
	newInfoCollector.slurmCommon = newSlurmCommon
	return newInfoCollector
}

// Describe sends metrics descriptions of this collector
// through the ch channel.
// It implements collector interface
func (ic *InfoCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- ic.partitionNodes
}

// Collect read the values of the metrics and
// passes them to the ch channel.
// It implements collector interface
func (ic *InfoCollector) Collect(ch chan<- prometheus.Metric) {
	log.Debugln("Collecting Info metrics...")
	var stdout, stderr bytes.Buffer
	var collected uint
	err := ic.slurmCommon.executeSSHCommand(
		infoCommand,
		&stdout,
		&stderr)

	if err != nil {
		log.Warnln(err.Error())
		return
	}

	// wait for stdout to fill (it is being filled async by ssh)
	time.Sleep(100 * time.Millisecond)

	nextLine := nextLineIterator(&stdout, sinfoLineParser)
	for fields, err := nextLine(); err == nil; fields, err = nextLine() {
		// check the line is correctly parsed
		if err != nil {
			log.Warnf(err.Error())
			continue
		}

		// send num of nodes

		// TODO(emepetres): three metrics, each for state in iSTATES
		// thin-interactive        up         1/1/0/2
		// "allocated/idle/other/total" should be in percentage!
		nodesByStatus := strings.Split(fields[iSTATES], "/")
		if len(nodesByStatus) != iSTATESNUMBER+1 {
			log.Warnln("Wrong parse of " + fields[iSTATES])
			continue
		}
		total, totalOk := strconv.ParseFloat(nodesByStatus[iSTATESNUMBER], 64)
		if totalOk != nil {
			log.Warnln(totalOk.Error())
			continue
		}

		var percentage float64
		for i := 0; i < iSTATESNUMBER; i++ {
			nodes, nodesOk := strconv.ParseFloat(nodesByStatus[i], 64)
			if nodesOk == nil {
				percentage = nodes / total * 100
				ch <- prometheus.MustNewConstMetric(
					ic.partitionNodes,
					prometheus.GaugeValue,
					percentage,
					fields[iPARTITION], fields[iAVAIL], iSTATESNAMES[i],
				)
			} else {
				log.Warnf(nodesOk.Error())
			}
		}
		collected++

	}
	log.Infof("%d partition info collected", collected)
}
