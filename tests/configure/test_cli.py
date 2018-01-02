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

import mock
from click.testing import CliRunner

from databricks_cli.configure.config import DEFAULT_SECTION, DatabricksConfig
import databricks_cli.configure.cli as cli


TEST_HOST = 'https://test.cloud.databricks.com'
TEST_USER = 'monkey@databricks.com'
TEST_PASSWORD = 'banana' # NOQA
TEST_TOKEN = 'dapiTESTING'

TEST_PROFILE = 'dev'
TEST_HOST_2 = 'https://test2.cloud.databricks.com'


def test_configure_cli(tmpdir):
    path = tmpdir.strpath
    with mock.patch.object(DatabricksConfig, 'home', path):
        runner = CliRunner()
        runner.invoke(cli.configure_cli,
                      input=(TEST_HOST + '\n' +
                             TEST_USER + '\n' +
                             TEST_PASSWORD + '\n' +
                             TEST_PASSWORD + '\n'))
        assert DatabricksConfig.fetch_from_fs().host(DEFAULT_SECTION) == TEST_HOST
        assert DatabricksConfig.fetch_from_fs().username(DEFAULT_SECTION) == TEST_USER
        assert DatabricksConfig.fetch_from_fs().password(DEFAULT_SECTION) == TEST_PASSWORD


def test_configure_cli_token(tmpdir):
    path = tmpdir.strpath
    with mock.patch.object(DatabricksConfig, 'home', path):
        runner = CliRunner()
        runner.invoke(cli.configure_cli, ['--token'],
                      input=(TEST_HOST + '\n' + TEST_TOKEN + '\n'))
        assert DatabricksConfig.fetch_from_fs().host(DEFAULT_SECTION) == TEST_HOST
        assert DatabricksConfig.fetch_from_fs().token(DEFAULT_SECTION) == TEST_TOKEN


def test_configure_two_sections(tmpdir):
    path = tmpdir.strpath
    with mock.patch.object(DatabricksConfig, 'home', path):
        runner = CliRunner()
        runner.invoke(cli.configure_cli, ['--token'],
                      input=(TEST_HOST + '\n' + TEST_TOKEN + '\n'))
        runner.invoke(cli.configure_cli, ['--token', '--profile', TEST_PROFILE],
                      input=(TEST_HOST_2 + '\n' + TEST_TOKEN + '\n'))
        assert DatabricksConfig.fetch_from_fs().host(DEFAULT_SECTION) == TEST_HOST
        assert DatabricksConfig.fetch_from_fs().token(DEFAULT_SECTION) == TEST_TOKEN
        assert DatabricksConfig.fetch_from_fs().host(TEST_PROFILE) == TEST_HOST_2
        assert DatabricksConfig.fetch_from_fs().token(TEST_PROFILE) == TEST_TOKEN
