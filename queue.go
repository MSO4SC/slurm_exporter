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
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/mso4sc/slurm_exporter/ssh"
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
func squeueLineParser(line string) ([]string, error) {
	// check if line is long enough
	if len(line) < 20*(qFIELDS-1)+1 {
		return nil, errors.New("Slurm line not long enough: \"" + line + "\"")
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

	return result, nil
}

const (
	sBOOTFAIL    = iota
	sCANCELLED   = iota
	sCOMPLETED   = iota
	sCONFIGURING = iota
	sCOMPLETING  = iota
	sFAILED      = iota
	sNODEFAIL    = iota
	sPENDING     = iota
	sPREEMPTED   = iota
	sREVOKED     = iota
	sRUNNING     = iota
	sSPECIALEXIT = iota
	sSTOPPED     = iota
	sSUSPENDED   = iota
	sTIMEOUT     = iota
)

// StatusDict maps string status with its int values
var StatusDict = map[string]float64{
	"BOOT_FAIL":    sBOOTFAIL,
	"CANCELLED":    sCANCELLED,
	"COMPLETED":    sCOMPLETED,
	"CONFIGURING":  sCONFIGURING,
	"COMPLETING":   sCOMPLETING,
	"FAILED":       sFAILED,
	"NODE_FAIL":    sNODEFAIL,
	"PENDING":      sPENDING,
	"PREEMPTED":    sPREEMPTED,
	"REVOKED":      sREVOKED,
	"RUNNING":      sRUNNING,
	"SPECIAL_EXIT": sSPECIALEXIT,
	"STOPPED":      sSTOPPED,
	"SUSPENDED":    sSUSPENDED,
	"TIMEOUT":      sTIMEOUT,
}

// QueueCollector collects metrics from the Slurm queues
type QueueCollector struct {
	waitTime *prometheus.Desc
	status   *prometheus.Desc
	host     string
	sshUser  string
	sshPass  string
	timeZone string
}

// NewQueueCollector creates a new Slurm Queue collector
func NewQueueCollector(sshHost, sshUser, sshPass, timeZone string) *QueueCollector {
	return &QueueCollector{
		waitTime: prometheus.NewDesc(
			"job_wait_time",
			"Time that the job waited, or is estimated to wait",
			[]string{"jobid", "name", "username", "partition", "numcpus"},
			nil,
		),
		status: prometheus.NewDesc(
			"job_status",
			"Status of the job",
			[]string{"jobid", "name", "username", "partition", "numcpus"},
			nil,
		),
		host:     sshHost,
		sshUser:  sshUser,
		sshPass:  sshPass,
		timeZone: timeZone,
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
	fmt.Println("Collecting metrics...")
	var stdout, stderr bytes.Buffer
	var collected uint
	err := qc.executeSSHCommand(
		queueCommand,
		&stdout,
		&stderr)

	if err != nil {
		fmt.Println(err)
		return
	}

	// wait for stdout to fill (it is being filled async by ssh)
	time.Sleep(100 * time.Millisecond)

	nextLine := nextLineIterator(&stdout, squeueLineParser)
	for fields, err := nextLine(); err == nil; fields, err = nextLine() {
		// check the line is correctly parsed
		if err != nil {
			log.Warnf(err.Error())
			continue
		}

		// parse submittime
		submittime, stErr := time.Parse(slurmLayout, fields[qSUBMITTIME]+qc.timeZone)
		if stErr != nil {
			log.Warn(stErr.Error())
			continue
		}

		// parse starttime and send wait time
		if fields[qSTARTTIME] != nullStartTime {
			starttime, sstErr := time.Parse(slurmLayout, fields[qSTARTTIME]+qc.timeZone)
			if sstErr == nil {
				waitTimestamp := starttime.Unix() - submittime.Unix()
				ch <- prometheus.MustNewConstMetric(
					qc.waitTime,
					prometheus.GaugeValue,
					float64(waitTimestamp),
					fields[qJOBID], fields[qNAME], fields[qUSERNAME],
					fields[qPARTITION], fields[qNUMCPUS],
				)
			} else {
				log.Warn(sstErr.Error())
			}
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
		} else {
			log.Warnf("Couldn't parse job status: %s", fields[qSTATE])
		}
	}
	fmt.Printf("...%d jobs collected.\n", collected)
}

func (qc *QueueCollector) executeSSHCommand(cmd string, stdout, stderr io.Writer) error {
	sshClient := ssh.NewSSHClientByPassword(
		qc.sshUser,
		qc.sshPass,
		qc.host,
		22,
	)

	command := &ssh.SSHCommand{
		Path: cmd,
		// Env:    []string{"LC_DIR=/usr"},
		Stdin:  nil,
		Stdout: stdout,
		Stderr: stderr,
	}

	return sshClient.RunCommand(command)
}

func parseSlurmTime(field string) (uint64, error) {
	var days, hours, minutes, seconds uint64
	var err error

	toParse := field
	haveDays := false

	// get days
	slice := strings.Split(toParse, "-")
	if len(slice) == 1 {
		toParse = slice[0]
	} else if len(slice) == 2 {
		days, err = strconv.ParseUint(slice[0], 10, 64)
		if err != nil {
			return 0, err
		}
		toParse = slice[1]
		haveDays = true
	} else {
		err = errors.New("slurm time could not be parsed: " + field)
	}

	// get hours, minutes and seconds
	slice = strings.Split(toParse, ":")
	if len(slice) == 3 {
		hours, err = strconv.ParseUint(slice[0], 10, 64)
		if err == nil {
			minutes, err = strconv.ParseUint(slice[1], 10, 64)
			if err == nil {
				seconds, err = strconv.ParseUint(slice[1], 10, 64)
			}
		}
		if err != nil {
			return 0, err
		}
	} else if len(slice) == 2 {
		if haveDays {
			hours, err = strconv.ParseUint(slice[0], 10, 64)
			if err == nil {
				minutes, err = strconv.ParseUint(slice[1], 10, 64)
			}
		} else {
			minutes, err = strconv.ParseUint(slice[0], 10, 64)
			if err == nil {
				seconds, err = strconv.ParseUint(slice[1], 10, 64)
			}
		}
		if err != nil {
			return 0, err
		}
	} else if len(slice) == 1 {
		if haveDays {
			hours, err = strconv.ParseUint(slice[0], 10, 64)
		} else {
			minutes, err = strconv.ParseUint(slice[0], 10, 64)
		}
		if err != nil {
			return 0, err
		}
	} else {
		err = errors.New("slurm time could not be parsed: " + field)
	}

	return days*24*60*60 + hours*60*60 + minutes*60 + seconds, nil
}

// nextLineIterator returns a function that iterates
// over an io.Reader object returning each line  parsed
// in fields following the parser method passed as argument
func nextLineIterator(buf io.Reader, parser func(string) ([]string, error)) func() ([]string, error) {
	var buffer = buf.(*bytes.Buffer)
	var parse = parser
	return func() ([]string, error) {
		// get next line in buffer
		line, err := buffer.ReadString('\n')
		if err != nil {
			return nil, err
		}

		// parse the line and return
		return parse(line)
	}
}
