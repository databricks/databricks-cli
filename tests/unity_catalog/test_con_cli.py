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

# pylint:disable=redefined-outer-name

import json
import mock
import pytest
from click.testing import CliRunner

from databricks_cli.unity_catalog import connection_cli
from tests.utils import provide_conf

CONNECTION_NAME = 'test_connection_name'
COMMENT = 'some_comment'

TESTHOST = "test_postgresql.fakedb.com"
TESTPORT = "1234"
TEST_OPTIONS = {
    "host": TESTHOST,
    "port": TESTPORT,
    "user": "user123",
    "password": "password123"
}

COMPLETE_OPTIONS = {
    'name': CONNECTION_NAME,
    'connection_type': 'MYSQL',
    'options': TEST_OPTIONS,
    'read_only': True,
    'comment': COMMENT,
}

RETURN_OPTIONS = {
    'name': CONNECTION_NAME,
    'connection_type': 1,
    'options': {"host": TESTHOST, "port": TESTPORT,},
    'read_only': True,
    'comment': COMMENT,
}



CONNECTION_TYPES = ['mysql']

# CONNECTION_TYPES = ['mysql', 'postresql', 'snowflake', 'redshift', 
#                    'sqldw', 'sqlserver', 'databricks', 'online-catalog']

@pytest.fixture()
def api_mock():
    with mock.patch(
            'databricks_cli.unity_catalog.connection_cli.UnityCatalogApi') as uc_api_mock:
        _connection_api_mock = mock.MagicMock()
        uc_api_mock.return_value = _connection_api_mock
        yield _connection_api_mock


@pytest.fixture()
def echo_mock():
    with mock.patch('databricks_cli.unity_catalog.connection_cli.click.echo') as echo_mock:
        yield echo_mock


@provide_conf
def test_create_connection_cli(api_mock):
    for con_type in CONNECTION_TYPES:
        api_mock.create_connection.return_value = RETURN_OPTIONS
        runner = CliRunner()
        runner.invoke(
            getattr(connection_cli, 'create_{0}_cli'.format(con_type)),
            args=[
                '--name', CONNECTION_NAME,
                '--host', TEST_OPTIONS['host'],
                '--port', TEST_OPTIONS['port'],
                '--user', TEST_OPTIONS['user'],
                '--read-only',
                '--comment', COMMENT,
            ], input='{0}\n{0}\n'.format(TEST_OPTIONS['password']))
        api_mock.create_connection.assert_called_once_with(COMPLETE_OPTIONS)
    

@provide_conf
def test_create_connection_cli_json(api_mock):
    api_mock.create_connection.return_value = RETURN_OPTIONS
    runner = CliRunner()
    runner.invoke(
        connection_cli.create_json,
        args=[
            '--json', json.dumps(COMPLETE_OPTIONS)
        ])
    api_mock.create_connection.assert_called_once_with(COMPLETE_OPTIONS)
