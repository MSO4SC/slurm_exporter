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

// Slurm exporter

package main

import (
	"flag"
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/log"
)

var (
	version = "20170925"

	addr = flag.String(
		"listen-address",
		":9100",
		"The address to listen on for HTTP requests.",
	)
	host = flag.String(
		"host",
		"localhost",
		"Slurm host slurm domain name or IP.",
	)
	sshUser = flag.String(
		"ssh-user",
		"",
		"SSH user for remote slurm connection (no localhost).",
	)
	sshPass = flag.String(
		"ssh-password",
		"",
		"SSH password for remote slurm connection (no localhost).",
	)
	countryTZ = flag.String(
		"countrytz",
		"Europe/Madrid",
		"Country Time zone of the host, (e.g. \"Europe/Madrid\").",
	)
)

func main() {
	fmt.Println("Version: " + version)
	flag.Parse()

	// Flags check
	if *host == "localhost" {
		flag.Usage()
		log.Fatalln("Localhost slurm connection not implemented yet.")
	} else {
		if *sshUser == "" {
			flag.Usage()
			log.Fatalln("An user must be provided to connect to Slurm remotely.")
		}
		if *sshPass == "" {
			flag.Usage()
			log.Warnln("A password should be provided to connect to Slurm remotely.")
		}
	}

	prometheus.MustRegister(NewQueueCollector(*host, *sshUser, *sshPass, *countryTZ))
	prometheus.MustRegister(NewInfoCollector(*host, *sshUser, *sshPass, *countryTZ))

	// Expose the registered metrics via HTTP.
	log.Infof("Starting Server: %s", *addr)
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(*addr, nil))
}
