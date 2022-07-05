#!/usr/bin/env bash
# Databricks CLI
# Copyright 2017 Databricks, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"), except
# that the use of services to which certain application programming
# interfaces (each, an "API") connect requires that the user first obtain
# a license for the use of the APIs from Databricks, Inc. ("Databricks"),
# by creating an account at www.databricks.com and agreeing to either (a)
# the Community Edition Terms of Service, (b) the Databricks Terms of
# Service, or (c) another written agreement between Licensee and Databricks
# for the use of the APIs.
#
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

# Make sure /tmp holds an up to date checkout of this repository.
databricks_cli_repo=https://github.com/databricks/databricks-cli
databricks_cli_path=/tmp/databricks_cli_main
echo "Updating $databricks_cli_repo in $databricks_cli_path..."
if [ ! -d $databricks_cli_path ]; then
    git clone $databricks_cli_repo $databricks_cli_path
else
    (
        cd $databricks_cli_path
        git pull --ff origin
    )
fi

# Run compatibility test given the reference checkout.
export DATABRICKS_CLI_MAIN_CHECKOUT=$databricks_cli_path
exec python3 -m pytest tests/test_compat.py
