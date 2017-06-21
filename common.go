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

// Slurm common functions

package main

import (
	"bytes"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/mso4sc/slurm_exporter/ssh"
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

// TrackedJobs represents the jobs currently being monitored
type TrackedJobs struct {
	queued    map[string]bool
	newQueued map[string]bool
	finished  map[string]bool
	mux       sync.Mutex
}

func (tj *TrackedJobs) startTracking() {
	tj.newQueued = make(map[string]bool)
}

func (tj *TrackedJobs) trackQueue(id string) {
	tj.newQueued[id] = true
	if _, ok := tj.queued[id]; ok {
		delete(tj.queued, id)
	}
}

func (tj *TrackedJobs) unTrackFinished(id string) {
	delete(tj.finished, id)
}

func (tj *TrackedJobs) finishTracking() {
	// merge what it left from queued on finished
	for k := range tj.queued {
		tj.finished[k] = true
	}
	tj.queued = tj.newQueued
	tj.newQueued = nil // free ram
}

func (tj *TrackedJobs) finishedJobs() []string {
	keys := make([]string, len(tj.finished))

	i := 0
	for k := range tj.finished {
		keys[i] = k
		i++
	}

	return keys
}

// SlurmCollector collects metrics from the Slurm queues
type SlurmCollector struct {
	host     string
	sshUser  string
	sshPass  string
	timeZone string
}

func (sc *SlurmCollector) executeSSHCommand(cmd string, stdout, stderr io.Writer) error {
	sshClient := ssh.NewSSHClientByPassword(
		sc.sshUser,
		sc.sshPass,
		sc.host,
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
