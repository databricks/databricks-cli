#!/usr/bin/env bash

set -ex

cd "$(dirname $0)"

prospector --profile ./prospector.yaml -t dodgy -t mccabe -t profile-validator -t pyflakes -t pylint
rstcheck README.rst
