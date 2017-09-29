#!/bin/bash

# Copyright 2017 MSO4SC - javier.carnero@atos.net
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if [[ $# < 3 ]] ; then
    echo 'Usage: '$0' -host=<HOST> -ssh-user=<USER> -ssh-password=<PASSWD> [-countrytz=<TZ>] [-log-level=<LOGLEVEL>]' 
    exit 1
fi

ARGS=$1' '$2' '$3
if [[ $# > 3 ]] ; then
	ARGS=$ARGS' '$4
fi
if [[ $# > 4 ]] ; then
	ARGS=$ARGS' '$5
fi

ID=$(docker run --rm -d -p 9100 mso4sc/slurm_exporter $ARGS)

# Get dynamic port in host
PORT=$(docker ps --no-trunc|grep $ID|sed 's/.*0.0.0.0://g'|sed 's/->.*//g')
#PORT=$(docker-compose port web 80) #Only if using docker-compose, This will give us something like 0.0.0.0:32790.

echo $ID $PORT
