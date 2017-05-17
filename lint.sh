#!/usr/bin/env bash

FWDIR="$(cd "`dirname $0`"; pwd)"
cd "$FWDIR"

prospector --profile "$FWDIR/prospector.yaml"
