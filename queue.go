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
	aJOBID    = iota
	aNAME     = iota
	aUSERNAME = iota
	aSTATE    = iota
	aFIELDS   = iota
)

const (
	slurmLayout   = time.RFC3339
	queueCommand  = "squeue -h -Ojobid,name,username,partition,numcpus,submittime,starttime,state -P"
	nullStartTime = "N/A"
	acctCommand   = "sacct -n -a -X -o \"JobIDRaw,JobName%%20,User%%20,State%%20\" -S%02d:%02d:%02d -sBF,CA,CD,CF,F,NF,PR,RS,S,TO | grep -v 'PENDING\\|COMPLETING\\|RUNNING'"
)

// TODO(emepetres): can be optimised doing at the same time Trim+alloc
func squeueLineParser(line string) []string {
	// check if line is long enough
	if len(line) < 20*(qFIELDS-1)+1 {
		log.Warnln("Slurm line not long enough: \"" + line + "\"")
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

func sacctLineParser(line string) []string {
	fields := strings.Fields(line)

	if len(fields) < aFIELDS {
		log.Warnf("sacct line parse failed (%s): %d fields expected, %d parsed", line, aFIELDS, len(fields))
		return nil
	}

	return fields
}

// QueueCollector collects metrics from the Slurm queues
type QueueCollector struct {
	waitTime          *prometheus.Desc
	status            *prometheus.Desc
	slurmCommon       *SlurmCollector
	alreadyRegistered []string
	lasttime          time.Time
}

// NewQueueCollector creates a new Slurm Queue collector
func NewQueueCollector(host, sshUser, sshPass, timeZone string) *QueueCollector {
	newQueueCollector := &QueueCollector{
		waitTime: prometheus.NewDesc(
			"job_wait_time",
			"Time that the job waited, or is estimated to wait",
			[]string{"jobid", "name", "username", "partition", "numcpus", "state"},
			nil,
		),
		status: prometheus.NewDesc(
			"job_status",
			"Status of the job",
			[]string{"jobid", "name", "username"},
			nil,
		),
		alreadyRegistered: make([]string, 0),
	}
	newSlurmCommon, err := NewSlurmCollector(host, sshUser, sshPass, timeZone)
	if err != nil {
		log.Fatalln(err.Error())
	}
	newQueueCollector.slurmCommon = newSlurmCommon
	newQueueCollector.setLastTime()
	return newQueueCollector
}

func (qc *QueueCollector) setLastTime() {
	qc.lasttime = time.Now().In(qc.slurmCommon.timeZone).Add(-1 * time.Minute)
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
	qc.collectAcct(ch)
	qc.collectQueue(ch)
}

func (qc *QueueCollector) collectAcct(ch chan<- prometheus.Metric) {
	log.Debugln("Collecting Acct metrics...")
	var stdout, stderr bytes.Buffer
	var collected uint

	hour := qc.lasttime.Hour()
	minute := qc.lasttime.Minute()
	second := qc.lasttime.Second()

	now := time.Now().In(qc.slurmCommon.timeZone)
	if now.Hour() < hour {
		hour = 0
		minute = 0
		second = 0
	}

	currentCommand := fmt.Sprintf(acctCommand, hour, minute, second)
	log.Debugln(currentCommand)

	err := qc.slurmCommon.executeSSHCommand(
		currentCommand,
		&stdout,
		&stderr)

	if err != nil {
		log.Errorf("sacct: %s", err.Error())
		return
	}

	// wait for stdout to fill (it is being filled async by ssh)
	time.Sleep(100 * time.Millisecond)
	qc.setLastTime()

	nextLine := nextLineIterator(&stdout, sacctLineParser)
	for fields, err := nextLine(); err == nil; fields, err = nextLine() {
		// check the line is correctly parsed
		if err != nil {
			log.Warnln(err.Error())
			continue
		}

		// parse and send job state
		status, statusOk := StatusDict[fields[aSTATE]]
		if statusOk {
			if jobIsNotInQueue(status) {
				ch <- prometheus.MustNewConstMetric(
					qc.status,
					prometheus.GaugeValue,
					float64(status),
					fields[aJOBID], fields[aNAME], fields[aUSERNAME],
				)
				qc.alreadyRegistered = append(qc.alreadyRegistered, fields[aJOBID])
				//log.Debugln("Job " + fields[aJOBID] + " finished with state " + fields[aSTATE])
				collected++
			}
		} else {
			log.Warnf("Couldn't parse job status: '%s', fields '%s'", fields[aSTATE], strings.Join(fields, "|"))
		}
	}

	log.Infof("%d finished jobs collected", collected)
}

func jobIsNotInQueue(state int) bool {
	return state != sPENDING && state != sRUNNING && state != sCOMPLETING
}

func (qc *QueueCollector) collectQueue(ch chan<- prometheus.Metric) {
	log.Debugln("Collecting Queue metrics...")
	var stdout, stderr bytes.Buffer
	var collected uint
	var currentCommand string

	if len(qc.alreadyRegistered) > 0 {
		currentCommand = fmt.Sprintf(queueCommand+" | grep -v '%s'", strings.Join(qc.alreadyRegistered, "\\|"))
		qc.alreadyRegistered = make([]string, 0) // free memory
	} else {
		currentCommand = queueCommand
	}
	log.Debugln(currentCommand)

	err := qc.slurmCommon.executeSSHCommand(
		currentCommand,
		&stdout,
		&stderr)

	if err != nil {
		msg := err.Error()
		possibleExitCode := msg[len(msg)-1:]
		if possibleExitCode != "1" {
			log.Errorln(msg)
		} else {
			log.Debugln("No queued jobs collected")
		}
		return
	}

	// wait for stdout to fill (it is being filled async by ssh)
	time.Sleep(100 * time.Millisecond)

	// remove already registered map memory from sacct when finished
	lastJob := ""
	nextLine := nextLineIterator(&stdout, squeueLineParser)
	for fields, err := nextLine(); err == nil; fields, err = nextLine() {
		// check the line is correctly parsed
		if err != nil {
			log.Warnln(err.Error())
			continue
		}

		// parse submittime
		submittime, stErr := time.Parse(slurmLayout, fields[qSUBMITTIME]+"Z")
		if stErr != nil {
			log.Warnln(stErr.Error())
			continue
		}

		// parse and send job state
		status, statusOk := StatusDict[fields[qSTATE]]
		if statusOk {
			if lastJob != fields[qJOBID] {
				ch <- prometheus.MustNewConstMetric(
					qc.status,
					prometheus.GaugeValue,
					float64(status),
					fields[qJOBID], fields[qNAME], fields[qUSERNAME],
				)
				lastJob = fields[qJOBID]
				collected++
			}

			// parse starttime and send wait time
			if fields[qSTARTTIME] != nullStartTime {
				starttime, sstErr := time.Parse(slurmLayout, fields[qSTARTTIME]+"Z")
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
	log.Infof("%d queued jobs collected", collected)
}
