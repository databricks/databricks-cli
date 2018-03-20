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
import click
from click.testing import CliRunner

import databricks_cli.configure.config as config
from databricks_cli.utils import InvalidConfigurationError
from tests.utils import provide_conf


@provide_conf
def test_provide_api_client():
    @click.command()
    @click.option('--x', required=True)
    @config.profile_option
    @config.provide_api_client
    def test_command(api_client, x): # noqa
        click.echo(x)

    result = CliRunner().invoke(test_command, ['--x', '1'])
    assert result.exit_code == 0
    assert result.output == '1\n'


def test_provide_api_client_invalid():
    @click.command()
    @click.option('--x', required=True)
    @config.profile_option
    @config.provide_api_client
    def test_command(api_client, x): # noqa
        click.echo(x)

    result = CliRunner().invoke(test_command, ['--x', '1'])
    assert result.exit_code == -1
    assert isinstance(result.exception, InvalidConfigurationError)


TEST_PROFILE_1 = 'test-profile-1'
TEST_PROFILE_2 = 'test-profile-2'


def test_provide_profile_twice():
    @click.group()
    @config.profile_option
    def test_group():
        pass

    @click.command()
    @config.profile_option
    def test_command(): # noqa
        pass

    test_group.add_command(test_command, 'test')

    result = CliRunner().invoke(test_group, ['--profile', TEST_PROFILE_1, 'test', '--profile',
                                             TEST_PROFILE_2])
    assert '--profile can only be provided once. The profiles [{}, {}] were provided.'.format(
        TEST_PROFILE_1, TEST_PROFILE_2) in result.output
