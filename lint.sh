#!/usr/bin/env bash

set -e

FWDIR="$(cd "`dirname $0`"; pwd)"
cd "$FWDIR"

prospector --profile "$FWDIR/prospector.yaml"

rstcheck README.rst
