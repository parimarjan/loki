# LOKI Data Generation

A collection of scripts to generate databases from high-level statistics about workloads.

## Usage

### Loki

1. Edit `application.conf` with the required details. A sample configuration and associated data are provided.
1. Execute `python loki.py -w <WORKLOAD> -t <TABLE> -n <VARS_PER_COL>`.

## Supported Input Formats

* CSV

## Authors

* Laurent Bindschaedler (bindscha@mit.edu)
* Parimarjan Negi (pnegi@mit.edu)

