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

import tempfile
from mock import patch
from click.testing import CliRunner

import databricks_cli.configure.cli as cli
from databricks_cli.configure.cli import ENV_AAD_TOKEN
from databricks_cli.configure.provider import get_config, ProfileConfigProvider

TEST_HOST = 'https://test.cloud.databricks.com'
TEST_USER = 'monkey@databricks.com'
TEST_PASSWORD = 'banana' # NOQA
TEST_TOKEN = 'dapiTESTING'

TEST_PROFILE = 'dev'
TEST_HOST_2 = 'https://test2.cloud.databricks.com'

TEST_SCOPES = 'offline_access,sql,accounts'


def test_configure_cli():
    runner = CliRunner()
    runner.invoke(cli.configure_cli,
                  input=(TEST_HOST + '\n' +
                         TEST_USER + '\n' +
                         TEST_PASSWORD + '\n' +
                         TEST_PASSWORD + '\n'))
    assert get_config().host == TEST_HOST
    assert get_config().username == TEST_USER
    assert get_config().password == TEST_PASSWORD


def test_configure_cli_token():
    runner = CliRunner()
    runner.invoke(cli.configure_cli, ['--token'],
                  input=(TEST_HOST + '\n' + TEST_TOKEN + '\n'))
    assert get_config().host == TEST_HOST
    assert get_config().token == TEST_TOKEN
    assert get_config().insecure is None


def test_configure_two_sections():
    runner = CliRunner()
    runner.invoke(cli.configure_cli, ['--token'],
                  input=(TEST_HOST + '\n' + TEST_TOKEN + '\n'))
    runner.invoke(cli.configure_cli, ['--token', '--profile', TEST_PROFILE],
                  input=(TEST_HOST_2 + '\n' + TEST_TOKEN + '\n'))
    assert get_config().host == TEST_HOST
    assert get_config().token == TEST_TOKEN
    assert ProfileConfigProvider(TEST_PROFILE).get_config().host == TEST_HOST_2
    assert ProfileConfigProvider(TEST_PROFILE).get_config().token == TEST_TOKEN


def test_configure_cli_insecure():
    runner = CliRunner()
    runner.invoke(cli.configure_cli, ['--token', '--insecure'],
                  input=(TEST_HOST + '\n' + TEST_TOKEN + '\n'))
    assert get_config().host == TEST_HOST
    assert get_config().token == TEST_TOKEN
    assert get_config().insecure == 'True'


def test_configure_cli_jobs_api_version():
    runner = CliRunner()
    runner.invoke(cli.configure_cli, ['--jobs-api-version', '2.1', '--token'],
                  input=(TEST_HOST + '\n' + TEST_TOKEN + '\n'))
    assert get_config().jobs_api_version == '2.1'


def test_configure_cli_jobs_api_version_aad_token():
    with patch.dict('os.environ', {ENV_AAD_TOKEN: 'token'}):
        runner = CliRunner()
        runner.invoke(cli.configure_cli, ['--jobs-api-version', '2.1', '--aad-token'],
                      input=(TEST_HOST + '\n'))
        assert get_config().jobs_api_version == '2.1'


def test_configure_cli_jobs_api_version_file():
    with tempfile.NamedTemporaryFile() as fp:
        fp.write(TEST_TOKEN.encode('utf-8'))
        fp.seek(0)

        runner = CliRunner()
        runner.invoke(cli.configure_cli, ['--jobs-api-version', '2.1', '--token-file', fp.name],
                      input=(TEST_HOST + '\n'))
        assert get_config().jobs_api_version == '2.1'
        assert get_config().token == TEST_TOKEN


def test_configure_cli_jobs_api_version_password():
    runner = CliRunner()
    runner.invoke(cli.configure_cli, ['--jobs-api-version', '2.1'],
                  input=(TEST_HOST + '\n' +
                         TEST_USER + '\n' +
                         TEST_PASSWORD + '\n' +
                         TEST_PASSWORD + '\n'))
    assert get_config().jobs_api_version == '2.1'
    assert get_config().host == TEST_HOST
    assert get_config().username == TEST_USER
    assert get_config().password == TEST_PASSWORD


def test_configure_cli_oauth():
    runner = CliRunner()
    runner.invoke(cli.configure_cli, ['--oauth'],
                  input=(TEST_HOST + '\n' + TEST_SCOPES + '\n'))
    # This is just exercising that the --oauth flag will parse the input as
    # expected. If we use it on a live OAuth endpoint, it will expect to
    # trigger a browser authentication flow. This is not going to be unit
    # testable from the command line.
