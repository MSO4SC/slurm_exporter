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

// sinfo Slurm auxiliary collector

package main

import (
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

const (
	iPARTITION = iota
	iAVAIL     = iota
	iSTATES    = iota
	iFIELDS    = iota
)

const (
	infoCommand   = "sinfo -h -o \"%20R %.5a %.15F\" | uniq"
	iSTATESNUMBER = 3
)

var iSTATESNAMES = [3]string{"allocated", "idle", "other"}

func (sc *SlurmCollector) collectInfo(ch chan<- prometheus.Metric) {
	log.Debugln("Collecting Info metrics...")
	var collected uint

	// execute the command
	log.Debugln(infoCommand)
	sshSession, err := sc.executeSSHCommand(infoCommand)
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
					sc.partitionNodes,
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

func sinfoLineParser(line string) []string {
	fields := strings.Fields(line)

	if len(fields) != iFIELDS {
		log.Warnf("sinfo line parse failed (%s): %d fields expected, %d parsed", line, iFIELDS, len(fields))
		return nil
	}

	return fields
}
