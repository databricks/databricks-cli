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

import databricks_cli.clusters.cli as cli
from databricks_cli.utils import pretty_format
from tests.utils import provide_conf

CREATE_RETURN = {'cluster_id': 'test'}
CREATE_JSON = '{"name": "test_cluster"}'
EDIT_JSON = '{"cluster_id": "test"}'


@pytest.fixture()
def cluster_api_mock():
    with mock.patch('databricks_cli.clusters.cli.ClusterApi') as ClusterApiMock:
        _cluster_api_mock = mock.MagicMock()
        ClusterApiMock.return_value = _cluster_api_mock
        yield _cluster_api_mock


@provide_conf
def test_create_cli_json(cluster_api_mock):
    with mock.patch('databricks_cli.jobs.cli.click.echo') as echo_mock:
        cluster_api_mock.create_cluster.return_value = CREATE_RETURN
        runner = CliRunner()
        runner.invoke(cli.create_cli, ['--json', CREATE_JSON])
        assert cluster_api_mock.create_cluster.call_args[0][0] == json.loads(CREATE_JSON)
        assert echo_mock.call_args[0][0] == pretty_format(CREATE_RETURN)


@provide_conf
def test_edit_cli_json(cluster_api_mock):
    runner = CliRunner()
    runner.invoke(cli.edit_cli, ['--json', EDIT_JSON])
    assert cluster_api_mock.edit_cluster.call_args[0][0] == json.loads(EDIT_JSON)


CLUSTER_ID = 'test'


@provide_conf
def test_start_cli(cluster_api_mock):
    runner = CliRunner()
    runner.invoke(cli.start_cli, ['--cluster-id', CLUSTER_ID])
    assert cluster_api_mock.start_cluster.call_args[0][0] == CLUSTER_ID


@provide_conf
def test_restart_cli(cluster_api_mock):
    runner = CliRunner()
    runner.invoke(cli.restart_cli, ['--cluster-id', CLUSTER_ID])
    assert cluster_api_mock.restart_cluster.call_args[0][0] == CLUSTER_ID


@provide_conf
def test_delete_cli(cluster_api_mock):
    runner = CliRunner()
    runner.invoke(cli.delete_cli, ['--cluster-id', CLUSTER_ID])
    assert cluster_api_mock.delete_cluster.call_args[0][0] == CLUSTER_ID


@provide_conf
def test_get_cli(cluster_api_mock):
    cluster_api_mock.get_cluster.return_value = '{}'
    runner = CliRunner()
    runner.invoke(cli.get_cli, ['--cluster-id', CLUSTER_ID])
    assert cluster_api_mock.get_cluster.call_args[0][0] == CLUSTER_ID


LIST_RETURN = {
    'clusters': [{
        'cluster_id': 'test_id',
        'cluster_name': 'test_name',
        'state': 'PENDING'
    }]
}


@provide_conf
def test_list_jobs(cluster_api_mock):
    with mock.patch('databricks_cli.clusters.cli.click.echo') as echo_mock:
        cluster_api_mock.list_clusters.return_value = LIST_RETURN
        runner = CliRunner()
        runner.invoke(cli.list_cli)
        assert echo_mock.call_args[0][0] == \
            tabulate([('test_id', 'test_name', 'PENDING')], tablefmt='plain')


@provide_conf
def test_list_clusters_output_json(cluster_api_mock):
    with mock.patch('databricks_cli.clusters.cli.click.echo') as echo_mock:
        cluster_api_mock.list_clusters.return_value = LIST_RETURN
        runner = CliRunner()
        runner.invoke(cli.list_cli, ['--output', 'json'])
        assert echo_mock.call_args[0][0] == pretty_format(LIST_RETURN)
