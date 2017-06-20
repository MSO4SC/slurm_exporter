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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
)

const (
	iPARTITION = iota
	iAVAIL     = iota
	iSTATE     = iota
	iNODES     = iota
	iFIELDS    = iota
)

const (
	infoCommand = "sinfo -h -o \"%12P %.5a %.6t %.6D\""
)

// InfoCollector collects metrics from Slurm info
type InfoCollector struct {
	partitionNodes *prometheus.Desc
	slurmCommon    SlurmCollector
}

// NewQueueCollector creates a new Slurm Queue collector
func NewInfoCollector(host, sshUser, sshPass, timeZone string) *InfoCollector {
	return &InfoCollector{
		partitionNodes: prometheus.NewDesc(
			"partition_nodes",
			"Nodes of the partition",
			[]string{"partition", "availability", "state"},
			nil,
		),
		slurmCommon: SlurmCollector{
			host:     host,
			sshUser:  sshUser,
			sshPass:  sshPass,
			timeZone: timeZone,
		},
	}
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
	fmt.Println("Collecting Info metrics...")
	var stdout, stderr bytes.Buffer
	var collected uint
	err := ic.slurmCommon.executeSSHCommand(
		infoCommand,
		&stdout,
		&stderr)

	if err != nil {
		fmt.Println(err)
		return
	}

	// wait for stdout to fill (it is being filled async by ssh)
	time.Sleep(100 * time.Millisecond)

	nextLine := nextLineIterator(&stdout, strings.Fields)
	for fields, err := nextLine(); err == nil; fields, err = nextLine() {
		// check the line is correctly parsed
		if err != nil {
			log.Warnf(err.Error())
			continue
		}

		// send num of nodes
		nodes, nodesOk := strconv.Atoi(fields[iNODES])
		if nodesOk == nil {
			ch <- prometheus.MustNewConstMetric(
				ic.partitionNodes,
				prometheus.GaugeValue,
				float64(nodes),
				fields[iPARTITION], fields[iAVAIL], fields[iSTATE],
			)
			collected++
		} else {
			log.Warnf(nodesOk.Error())
		}
	}
	fmt.Printf("...%d partition nodes info collected.\n", collected)
}
