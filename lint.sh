#!/usr/bin/env bash

set -ex

PY_VERSION=${1:-3}

FWDIR="$(cd "`dirname $0`"; pwd)"
cd "$FWDIR"

if [[ "$PY_VERSION" -eq 3 ]]; then
  prospector --profile "$FWDIR/prospector.yaml"
else
  prospector --profile "$FWDIR/prospector-2.yaml"
fi

rstcheck README.rst
