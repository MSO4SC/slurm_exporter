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
	log "github.com/sirupsen/logrus"
)

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
var StatusDict = map[string]int{
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
	iPARTITION = iota
	iAVAIL     = iota
	iSTATES    = iota
	iFIELDS    = iota
)

const (
	slurmLayout   = time.RFC3339
	queueCommand  = "squeue -h -Ojobid,name,username,partition,numcpus,submittime,starttime,state -P"
	nullStartTime = "N/A"
	acctCommand   = "sacct -n -a -X -o \"JobIDRaw,JobName%%20,User%%20,State%%20\" -S%02d:%02d:%02d -sBF,CA,CD,CF,F,NF,PR,RS,S,TO | grep -v 'PENDING\\|COMPLETING\\|RUNNING'"
	infoCommand   = "sinfo -h -o \"%20P %.5a %.15F\""
	iSTATESNUMBER = 3
)

var iSTATESNAMES = [3]string{"allocated", "idle", "other"}

// QueueCollector collects metrics from the Slurm queues
type QueueCollector struct {
	waitTime          *prometheus.Desc
	status            *prometheus.Desc
	partitionNodes    *prometheus.Desc
	sshConfig         *ssh.SSHConfig
	sshClient         *ssh.SSHClient
	timeZone          *time.Location
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
		partitionNodes: prometheus.NewDesc(
			"partition_nodes",
			"Nodes of the partition",
			[]string{"partition", "availability", "state"},
			nil,
		),
		sshConfig: ssh.NewSSHConfigByPassword(
			sshUser,
			sshPass,
			host,
			22,
		),
		sshClient:         nil,
		alreadyRegistered: make([]string, 0),
	}
	var err error
	newQueueCollector.timeZone, err = time.LoadLocation(timeZone)
	if err != nil {
		log.Fatalln(err.Error())
	}
	newQueueCollector.setLastTime()
	return newQueueCollector
}

// Describe sends metrics descriptions of this collector
// through the ch channel.
// It implements collector interface
func (qc *QueueCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- qc.waitTime
	ch <- qc.status
	ch <- qc.partitionNodes
}

// Collect read the values of the metrics and
// passes them to the ch channel.
// It implements collector interface
func (qc *QueueCollector) Collect(ch chan<- prometheus.Metric) {
	var err error
	qc.sshClient, err = qc.sshConfig.NewClient()
	if err != nil {
		log.Errorf("Creating SSH client: %s", err.Error())
		return
	}

	qc.collectAcct(ch)
	qc.collectQueue(ch)
	qc.collectInfo(ch)

	err = qc.sshClient.Close()
	if err != nil {
		log.Errorf("Closing SSH client: %s", err.Error())
	}
}

func (qc *QueueCollector) collectAcct(ch chan<- prometheus.Metric) {
	log.Debugln("Collecting Acct metrics...")
	var collected uint

	hour := qc.lasttime.Hour()
	minute := qc.lasttime.Minute()
	second := qc.lasttime.Second()

	now := time.Now().In(qc.timeZone)
	if now.Hour() < hour {
		hour = 0
		minute = 0
		second = 0
	}

	currentCommand := fmt.Sprintf(acctCommand, hour, minute, second)
	log.Debugln(currentCommand)

	sshSession, err := qc.executeSSHCommand(currentCommand)
	if sshSession != nil {
		defer sshSession.Close()
	}
	if err != nil {
		log.Errorf("sacct: %s", err.Error())
		return
	}

	// wait for stdout to fill (it is being filled async by ssh)
	time.Sleep(100 * time.Millisecond)
	qc.setLastTime()

	nextLine := nextLineIterator(sshSession.OutBuffer, sacctLineParser)
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

func (qc *QueueCollector) collectQueue(ch chan<- prometheus.Metric) {
	log.Debugln("Collecting Queue metrics...")
	var collected uint
	var currentCommand string

	if len(qc.alreadyRegistered) > 0 {
		currentCommand = fmt.Sprintf(queueCommand+" | grep -v '%s'", strings.Join(qc.alreadyRegistered, "\\|"))
		qc.alreadyRegistered = make([]string, 0) // free memory
	} else {
		currentCommand = queueCommand
	}

	// execute the command
	log.Debugln(currentCommand)
	sshSession, err := qc.executeSSHCommand(currentCommand)
	if sshSession != nil {
		defer sshSession.Close()
	}
	if err != nil {
		if sshSession != nil {
			msg := err.Error()
			possibleExitCode := msg[len(msg)-1:]
			if possibleExitCode != "1" {
				log.Errorln(msg)
			} else {
				log.Debugln("No queued jobs collected")
				//TODO(emepetres) ¿¿supply metrics when no job data is available??
			}
			return
		}
		log.Errorln(err.Error())
		return
	}

	// wait for stdout to fill (it is being filled async by ssh)
	time.Sleep(100 * time.Millisecond)

	// remove already registered map memory from sacct when finished
	lastJob := ""
	nextLine := nextLineIterator(sshSession.OutBuffer, squeueLineParser)
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

func (qc *QueueCollector) collectInfo(ch chan<- prometheus.Metric) {
	log.Debugln("Collecting Info metrics...")
	var collected uint

	// execute the command
	log.Debugln(infoCommand)
	sshSession, err := qc.executeSSHCommand(infoCommand)
	if sshSession != nil {
		defer sshSession.Close()
	}
	if err != nil {
		log.Warnln(err.Error())
		return
	}

	// wait for stdout to fill (it is being filled async by ssh)
	time.Sleep(100 * time.Millisecond)

	nextLine := nextLineIterator(sshSession.OutBuffer, sinfoLineParser)
	for fields, err := nextLine(); err == nil; fields, err = nextLine() {
		// check the line is correctly parsed
		if err != nil {
			log.Warnf(err.Error())
			continue
		}

		// send num of nodes per state and partition
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
					qc.partitionNodes,
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

func (qc *QueueCollector) executeSSHCommand(cmd string) (*ssh.SSHSession, error) {
	command := &ssh.SSHCommand{
		Path: cmd,
		// Env:    []string{"LC_DIR=/usr"},
	}

	var outb, errb bytes.Buffer
	session, err := qc.sshClient.OpenSession(nil, &outb, &errb)
	if err == nil {
		err = session.RunCommand(command)
		return session, err
	}
	return nil, err
}

func (qc *QueueCollector) setLastTime() {
	qc.lasttime = time.Now().In(qc.timeZone).Add(-1 * time.Minute)
}

func jobIsNotInQueue(state int) bool {
	return state != sPENDING && state != sRUNNING && state != sCOMPLETING
}

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

func sinfoLineParser(line string) []string {
	fields := strings.Fields(line)

	if len(fields) != iFIELDS {
		log.Warnf("sinfo line parse failed (%s): %d fields expected, %d parsed", line, iFIELDS, len(fields))
		return nil
	}

	return fields
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
		err = errors.New("Slurm time could not be parsed: " + field)
		return 0, err
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
		err = errors.New("Slurm time could not be parsed: " + field)
		return 0, err
	}

	return days*24*60*60 + hours*60*60 + minutes*60 + seconds, nil
}

// nextLineIterator returns a function that iterates
// over an io.Reader object returning each line  parsed
// in fields following the parser method passed as argument
func nextLineIterator(buf io.Reader, parser func(string) []string) func() ([]string, error) {
	var buffer = buf.(*bytes.Buffer)
	var parse = parser
	return func() ([]string, error) {
		// get next line in buffer
		line, err := buffer.ReadString('\n')
		if err != nil {
			return nil, err
		}

		// parse the line and return
		parsed := parse(line)
		if parsed == nil {
			return nil, errors.New("not able to parse line")
		}
		return parsed, nil
	}
}
