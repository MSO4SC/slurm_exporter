# Slurm Prometheus Exporter

Allow a server to collect metrics from SLURM and expose them in Prometheus format. The exporter accesses SLURM by ssh to a remote machine that can perform `sacct`, `squeue`, and `sinfo` commands.

Running it on the same machine as SLURM is not currently supported.

## Install

> Requires Go >=1.8

```
go get github.com/mso4sc/slurm_exporter
$GOPATH/src/github.com/mso4sc/slurm_exporter/utils/install.sh
```

## Usage

```
slurm_exporter -host=<HOST> -ssh-user=<USER> -ssh-password=<PASSWD> [-listen-address=:<PORT>] [-countrytz=<TZ>] [-log.level=<LOGLEVEL>]
```

### Defaults

\<PORT\>: `:9100`  
\<HOST\>: `localhost`, not supported  
\<TZ\>: `Europe/Madrid`  
\<LOGLEVEL\>: `error`  

## Docker

https://hub.docker.com/r/mso4sc/slurm_exporter/

