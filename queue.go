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

// Queue Slurm collector

package main

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
)

const (
	qJOBID      = iota
	qNAME       = iota
	qUSERNAME   = iota
	qPARTITION  = iota
	qNUMCPUS    = iota
	qSUBMITTIME = iota
	qSTARTTIME  = iota
	qSTATE      = iota
	qFIELDS     = iota
)

const (
	slurmLayout   = time.RFC3339
	queueCommand  = "squeue -h -O \"jobid,name,username,partition,numcpus,submittime,starttime,state\""
	nullStartTime = "N/A"
)

// TODO(emepetres): can be optimised doing at the same time Trim+alloc
func squeueLineParser(line string) []string {
	// check if line is long enough
	if len(line) < 20*(qFIELDS-1)+1 {
		log.Warnf("Slurm line not long enough: \"" + line + "\"")
		return nil
	}

	// separate fields by 20 chars, trimming them
	result := make([]string, 0, qFIELDS)
	for i := 0; i < qFIELDS-1; i++ {
		result = append(result, strings.TrimSpace(line[20*i:20*(i+1)]))
	}
	result = append(result, strings.TrimSpace(line[20*(qFIELDS-1):]))

	// add + to the end of the name if it is long enough
	if len(result[qNAME]) == 20 {
		result[qNAME] = result[qNAME][:19] + "+"
	}

	return result
}

// QueueCollector collects metrics from the Slurm queues
type QueueCollector struct {
	waitTime    *prometheus.Desc
	status      *prometheus.Desc
	slurmCommon SlurmCollector
	trackedJobs *TrackedJobs
}

// NewQueueCollector creates a new Slurm Queue collector
func NewQueueCollector(host, sshUser, sshPass, timeZone string, trackedJobs *TrackedJobs) *QueueCollector {
	return &QueueCollector{
		waitTime: prometheus.NewDesc(
			"job_wait_time",
			"Time that the job waited, or is estimated to wait",
			[]string{"jobid", "name", "username", "partition", "numcpus", "state"},
			nil,
		),
		status: prometheus.NewDesc(
			"job_status",
			"Status of the job",
			[]string{"jobid", "name", "username", "partition", "numcpus"},
			nil,
		),
		slurmCommon: SlurmCollector{
			host:     host,
			sshUser:  sshUser,
			sshPass:  sshPass,
			timeZone: timeZone,
		},
		trackedJobs: trackedJobs,
	}
}

// Describe sends metrics descriptions of this collector
// through the ch channel.
// It implements collector interface
func (qc *QueueCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- qc.waitTime
	ch <- qc.status
}

// Collect read the values of the metrics and
// passes them to the ch channel.
// It implements collector interface
func (qc *QueueCollector) Collect(ch chan<- prometheus.Metric) {
	fmt.Println("Collecting Queue metrics...")
	var stdout, stderr bytes.Buffer
	var collected uint
	err := qc.slurmCommon.executeSSHCommand(
		queueCommand,
		&stdout,
		&stderr)

	if err != nil {
		fmt.Println(err)
		return
	}

	// wait for stdout to fill (it is being filled async by ssh)
	time.Sleep(100 * time.Millisecond)

	//Lock the tracked jobs while we are working
	qc.trackedJobs.mux.Lock()
	defer qc.trackedJobs.mux.Unlock()
	qc.trackedJobs.startTracking()

	nextLine := nextLineIterator(&stdout, squeueLineParser)
	for fields, err := nextLine(); err == nil; fields, err = nextLine() {
		// check the line is correctly parsed
		if err != nil {
			log.Warnf(err.Error())
			continue
		}

		// parse submittime
		submittime, stErr := time.Parse(slurmLayout, fields[qSUBMITTIME]+qc.slurmCommon.timeZone)
		if stErr != nil {
			log.Warn(stErr.Error())
			continue
		}

		// parse and send job state
		status, statusOk := StatusDict[fields[qSTATE]]
		if statusOk {
			ch <- prometheus.MustNewConstMetric(
				qc.status,
				prometheus.GaugeValue,
				status,
				fields[qJOBID], fields[qNAME], fields[qUSERNAME],
				fields[qPARTITION], fields[qNUMCPUS],
			)
			collected++

			// track the job
			qc.trackedJobs.track(fields[qJOBID])

			// parse starttime and send wait time
			if fields[qSTARTTIME] != nullStartTime {
				starttime, sstErr := time.Parse(slurmLayout, fields[qSTARTTIME]+qc.slurmCommon.timeZone)
				if sstErr == nil {
					waitTimestamp := starttime.Unix() - submittime.Unix()
					ch <- prometheus.MustNewConstMetric(
						qc.waitTime,
						prometheus.GaugeValue,
						float64(waitTimestamp),
						fields[qJOBID], fields[qNAME], fields[qUSERNAME],
						fields[qPARTITION], fields[qNUMCPUS], fields[qSTATE],
					)
				} else {
					log.Warn(sstErr.Error())
				}
			}
		} else {
			log.Warnf("Couldn't parse job status: %s", fields[qSTATE])
		}
	}
	fmt.Printf("...%d jobs collected.\n", collected)

	// finish job tracking
	qc.trackedJobs.finishTracking()
}
