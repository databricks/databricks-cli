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

# pylint:disable=protected-access

from click.testing import CliRunner
from tabulate import tabulate

import databricks_cli.profiles.cli as cli
from tests.utils import provide_conf


LIST_RETURN = [
    ['DEFAULT', 'test-host'],
    ['test-profile', 'test-host-2'],
    [''],
]

QUIET_LIST_RETURN = [
    'DEFAULT',
    'test-profile',
    '',
]


@provide_conf
def test_profiles_list():
    runner = CliRunner()
    stdout = runner.invoke(cli.list_cli).stdout
    stdout_lines = stdout.split('\n')
    expected = tabulate(LIST_RETURN, tablefmt='plain').split('\n')
    assert stdout_lines == expected


@provide_conf
def test_profiles_list_quiet():
    runner = CliRunner()
    stdout = runner.invoke(cli.list_cli, ['--quiet']).stdout
    stdout_lines = stdout.split('\n')
    assert stdout_lines == QUIET_LIST_RETURN
