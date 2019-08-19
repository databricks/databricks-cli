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
from tabulate import tabulate
from click.testing import CliRunner

import databricks_cli.instance_pools.cli as cli
from databricks_cli.utils import pretty_format
from tests.utils import provide_conf

CREATE_RETURN = {'instance_pool_id': 'test_id'}
CREATE_JSON = '{"instance_pool_name": "test_name"}'
EDIT_JSON = '{"instance_pool_id": "test_id"}'


@pytest.fixture()
def instance_pool_api_mock():
    with mock.patch('databricks_cli.instance_pools.cli.InstancePoolsApi') as InstancePoolsApiMock:
        _instance_pool_api_mock = mock.MagicMock()
        InstancePoolsApiMock.return_value = _instance_pool_api_mock
        yield _instance_pool_api_mock


@provide_conf
def test_create_cli_json(instance_pool_api_mock):
    with mock.patch('databricks_cli.jobs.cli.click.echo') as echo_mock:
        instance_pool_api_mock.create_instance_pool.return_value = CREATE_RETURN
        runner = CliRunner()
        runner.invoke(cli.create_cli, ['--json', CREATE_JSON])
        assert instance_pool_api_mock.create_instance_pool.call_args[0][0] == \
            json.loads(CREATE_JSON)
        assert echo_mock.call_args[0][0] == pretty_format(CREATE_RETURN)


@provide_conf
def test_edit_cli_json(instance_pool_api_mock):
    runner = CliRunner()
    runner.invoke(cli.edit_cli, ['--json', EDIT_JSON])
    assert instance_pool_api_mock.edit_instance_pool.call_args[0][0] == json.loads(EDIT_JSON)


INSTANCE_POOL_ID = 'test'


@provide_conf
def test_delete_cli(instance_pool_api_mock):
    runner = CliRunner()
    runner.invoke(cli.delete_cli, ['--instance-pool-id', INSTANCE_POOL_ID])
    assert instance_pool_api_mock.delete_instance_pool.call_args[0][0] == INSTANCE_POOL_ID


@provide_conf
def test_get_cli(instance_pool_api_mock):
    instance_pool_api_mock.get_instance_pool.return_value = '{}'
    runner = CliRunner()
    runner.invoke(cli.get_cli, ['--instance-pool-id', INSTANCE_POOL_ID])
    assert instance_pool_api_mock.get_instance_pool.call_args[0][0] == INSTANCE_POOL_ID


LIST_RETURN = {
    'instance_pools': [{
        'instance_pool_id': 'test_id',
        'instance_pool_name': 'test_name',
        "stats": {
            "idle_count": 1,
            "pending_used_count": 2,
            "pending_idle_count": 3,
            "used_count": 4
        }
    }]
}


@provide_conf
def test_list_jobs(instance_pool_api_mock):
    with mock.patch('databricks_cli.instance_pools.cli.click.echo') as echo_mock:
        instance_pool_api_mock.list_instance_pools.return_value = LIST_RETURN
        runner = CliRunner()
        runner.invoke(cli.list_cli)
        headers = ['ID', 'NAME', 'IDLE INSTANCES', 'USED INSTANCES', 'PENDING IDLE INSTANCES',
                   'PENDING USED INSTANCES']
        assert echo_mock.call_args[0][0] == tabulate([('test_id', 'test_name', 1, 4, 3, 2)],
                                                     headers=headers, tablefmt='plain',
                                                     numalign='left')


@provide_conf
def test_list_instance_pool_output_json(instance_pool_api_mock):
    with mock.patch('databricks_cli.instance_pools.cli.click.echo') as echo_mock:
        instance_pool_api_mock.list_instance_pools.return_value = LIST_RETURN
        runner = CliRunner()
        runner.invoke(cli.list_cli, ['--output', 'json'])
        assert echo_mock.call_args[0][0] == pretty_format(LIST_RETURN)
