# Slurm Prometheus Exporter

Server that collects metrics from Slurm and exposes them in the Prometheus format.

## Requirements


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

<PORT>: `:9100`
<HOST>: `localhost`, not supported
<TZ>: `Europe/Madrid`
<LOGLEVEL>: `error`
