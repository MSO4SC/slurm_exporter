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

// sacct Slurm auxiliary collector

package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

const (
	aJOBID     = iota
	aNAME      = iota
	aUSERNAME  = iota
	aPARTITION = iota
	aSTATE     = iota
	aFIELDS    = iota
)

const (
	acctCommand = "sacct -n -a -X -o \"JobIDRaw,JobName%%20,User%%20,Partition%%20,State%%20\" -S%02d:%02d:%02d -sBF,CA,CD,CF,F,NF,PR,RS,S,TO | grep -v 'PENDING\\|COMPLETING\\|RUNNING'"
)

func (sc *SlurmCollector) collectAcct(ch chan<- prometheus.Metric) {
	log.Debugln("Collecting Acct metrics...")
	var collected uint

	hour := sc.lasttime.Hour()
	minute := sc.lasttime.Minute()
	second := sc.lasttime.Second()

	now := time.Now().In(sc.timeZone)
	if now.Hour() < hour {
		hour = 0
		minute = 0
		second = 0
	}

	currentCommand := fmt.Sprintf(acctCommand, hour, minute, second)
	log.Debugln(currentCommand)

	sshSession, err := sc.executeSSHCommand(currentCommand)
	if sshSession != nil {
		defer sshSession.Close()
	}
	if err != nil {
		log.Errorf("sacct: %s", err.Error())
		return
	}

	// wait for stdout to fill (it is being filled async by ssh)
	time.Sleep(100 * time.Millisecond)
	sc.setLastTime()

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
					sc.status,
					prometheus.GaugeValue,
					float64(status),
					fields[aJOBID], fields[aNAME], fields[aUSERNAME], fields[aPARTITION],
				)
				sc.alreadyRegistered = append(sc.alreadyRegistered, fields[aJOBID])
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

func sacctLineParser(line string) []string {
	fields := strings.Fields(line)

	if len(fields) < aFIELDS {
		log.Warnf("sacct line parse failed (%s): %d fields expected, %d parsed", line, aFIELDS, len(fields))
		return nil
	}

	return fields
}
