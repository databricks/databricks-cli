#!/usr/bin/env bash

set -ex

PY_VERSION=${1:-3}

FWDIR="$(cd "`dirname $0`"; pwd)"
cd "$FWDIR"

tools="-t dodgy -t mccabe -t profile-validator -t pyflakes -t pylint"
if [[ "$PY_VERSION" -eq 3 ]]; then
  prospector --profile "$FWDIR/prospector.yaml" $tools
else
  prospector --profile "$FWDIR/prospector-2.yaml" $tools
fi

rstcheck README.rst
