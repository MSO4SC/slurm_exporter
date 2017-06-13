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

// var x = regexp.MustCompile("^([0-9]+)-([0-9]?[0-9]):([0-9][0-9]):([0-9][0-9])$")

// var TimeLayouts = []string{
// 	"1-15:04:05",
// 	"4-15:04",
// 	"4-15",
// 	"15:04:05",
// 	"04:05",
// 	"04",
// }

const (
	BOOTFAIL    = iota
	CANCELLED   = iota
	COMPLETED   = iota
	CONFIGURING = iota
	COMPLETING  = iota
	FAILED      = iota
	NODEFAIL    = iota
	PENDING     = iota
	PREEMPTED   = iota
	REVOKED     = iota
	RUNNING     = iota
	SPECIALEXIT = iota
	STOPPED     = iota
	SUSPENDED   = iota
	TIMEOUT     = iota
)

// StatusDict maps string status with its int values
var StatusDict = map[string]float64{
	"BOOT_FAIL":    BOOTFAIL,
	"CANCELLED":    CANCELLED,
	"COMPLETED":    COMPLETED,
	"CONFIGURING":  CONFIGURING,
	"COMPLETING":   COMPLETING,
	"FAILED":       FAILED,
	"NODE_FAIL":    NODEFAIL,
	"PENDING":      PENDING,
	"PREEMPTED":    PREEMPTED,
	"REVOKED":      REVOKED,
	"RUNNING":      RUNNING,
	"SPECIAL_EXIT": SPECIALEXIT,
	"STOPPED":      STOPPED,
	"SUSPENDED":    SUSPENDED,
	"TIMEOUT":      TIMEOUT,
}

// QueueCollector collects metrics from the Slurm queues
type QueueCollector struct {
	executionTime *prometheus.Desc
	status        *prometheus.Desc
}

// NewQueueCollector creates a new Slurm Queue collector
func NewQueueCollector() *QueueCollector {
	return &QueueCollector{
		executionTime: prometheus.NewDesc(
			"job_execution_time",
			"Time that the job is executing",
			[]string{"jobid", "partition", "nodes"},
			nil,
		),
		status: prometheus.NewDesc(
			"job_status",
			"Status of the job",
			[]string{"jobid", "partition", "nodes"},
			nil,
		),
	}
}

// Describe sends metrics descriptions of this collector
// through the ch channel.
// It implements collector interface
func (qc *QueueCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- qc.executionTime
	ch <- qc.status
}

// Collect read the values of the metrics and
// passes them to the ch channel.
// It implements collector interface
func (qc *QueueCollector) Collect(ch chan<- prometheus.Metric) {
	fmt.Println("Collecting metrics...")
	var stdout, stderr bytes.Buffer
	var collected uint
	err := executeSSHCommand(
		"squeue -h -o \"%.10A %.9P %.12T %.10M %.6D\"",
		&stdout,
		&stderr)

	if err != nil {
		fmt.Println(err)
		return
	}

	// wait for stdout to fill (it is being filled async by ssh)
	time.Sleep(100 * time.Millisecond)

	nextLine := nextLineIterator(&stdout)
	for fields, err := nextLine(); err == nil; fields, err = nextLine() {
		if len(fields) >= 5 {
			ts, tsErr := parseSlurmTime(fields[3])
			if tsErr == nil {
				ch <- prometheus.MustNewConstMetric(
					qc.executionTime,
					prometheus.CounterValue,
					float64(ts),
					fields[0], fields[1], fields[4],
				)
			} else {
				log.Warnf("Couldn't parse job execution time (%s): %s", strings.Join(fields, ","), tsErr.Error())
			}

			status, statusOk := StatusDict[fields[2]]
			if statusOk {
				ch <- prometheus.MustNewConstMetric(
					qc.status,
					prometheus.CounterValue,
					status,
					fields[0], fields[1], fields[4],
				)
			} else {
				log.Warnf("Couldn't parse job status: %s", fields[2])
			}
			collected++
		} else {
			log.Warnf("Not enough fields to parse: %s", strings.Join(fields, ","))
		}
	}
	fmt.Printf("...%d jobs collected.\n", collected)
}

func executeSSHCommand(cmd string, stdout, stderr io.Writer) error {
	sshClient := ssh.NewSSHClientByPassword(
		"otarijci",
		"300tt.yo",
		"ft2.cesga.es",
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
// over an io.Reader object returning each line in fields
// separated by spaces
func nextLineIterator(buf io.Reader) func() ([]string, error) {
	var buffer = buf.(*bytes.Buffer)
	return func() ([]string, error) {
		line, err := buffer.ReadString('\n')
		return strings.Fields(line), err
	}
}
