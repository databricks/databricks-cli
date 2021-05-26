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
import json
import mock
import click
from click.testing import CliRunner

import databricks_cli.configure.config as config
from databricks_cli.utils import InvalidConfigurationError, eat_exceptions
from databricks_cli.configure.provider import DatabricksConfig
from databricks_cli.click_types import ContextObject
from tests.utils import provide_conf


@provide_conf
def test_debug_option():
    # Test that context object debug_mode property changes with --debug flag fed.
    @click.command()
    @click.option('--debug-fed', type=bool)
    @config.debug_option
    def test_debug(debug_fed): # noqa
        ctx = click.get_current_context()
        context_object = ctx.ensure_object(ContextObject)
        assert context_object.debug_mode is debug_fed
    result = CliRunner().invoke(test_debug, ['--debug', '--debug-fed', True])
    assert result.exit_code == 0
    result = CliRunner().invoke(test_debug, ['--debug-fed', False])
    assert result.exit_code == 0

    # Test that with eat_exceptions wrapper, 'Traceback' appears or doesn't appear depending on
    # whether --debug flag is given.
    @click.command()
    @config.debug_option
    @eat_exceptions
    def test_debug_traceback():  # noqa
        assert False
    result = CliRunner().invoke(test_debug_traceback, ['--debug'])
    assert result.exit_code == 1
    assert 'Traceback' in result.output
    result = CliRunner().invoke(test_debug_traceback)
    assert result.exit_code == 1
    assert 'Traceback' not in result.output


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
    assert result.exit_code == 1
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


TEST_HOST = 'https://test.cloud.databricks.com'
TEST_TOKEN = 'testtoken'


def test_command_headers():
    @click.group()
    @config.profile_option
    def outer_test_group():
        pass

    @click.group()
    @config.profile_option
    def inner_test_group():
        pass

    @click.command()
    @click.option('--x', required=True)
    @config.profile_option
    @config.provide_api_client
    def test_command(api_client, x): # noqa
        click.echo(json.dumps(api_client.default_headers))

    with mock.patch("databricks_cli.configure.config.get_config") as config_mock:
        with mock.patch("uuid.uuid1") as uuid_mock:
            config_mock.return_value = DatabricksConfig.from_token(TEST_HOST, TEST_TOKEN)
            uuid_mock.return_value = '1234'
            inner_test_group.add_command(test_command, 'subcommand')
            outer_test_group.add_command(inner_test_group, 'command')
            result = CliRunner().invoke(outer_test_group, ['command', 'subcommand', '--x', '12'])
            assert result.exception is None
            default_headers = json.loads(result.output)
            assert 'user-agent' in default_headers
            assert "command-subcommand-1234" in default_headers['user-agent']
