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
from tabulate import tabulate

import databricks_cli.clusters.cli as cli
from databricks_cli.utils import pretty_format
from tests.test_data import TEST_CLUSTER_ID, TEST_CLUSTER_NAME, \
    CLUSTERS_BY_NAME_SINGLE_CLUSTER_RV, CLUSTERS_BY_NAME_MULTIPLE_CLUSTER_RV, \
    MULTIPLE_CLUSTERS_FAILURE_OUTPUT
from tests.utils import provide_conf, assert_cli_output

CLUSTER_ID = TEST_CLUSTER_ID
CLUSTER_NAME = TEST_CLUSTER_NAME

CREATE_RETURN = {'cluster_id': TEST_CLUSTER_ID}
CREATE_JSON = '{{"name": "{}"}}'.format(TEST_CLUSTER_NAME)
EDIT_JSON = '{{"cluster_id": "{}"}}'.format(TEST_CLUSTER_ID)


@pytest.fixture()
def cluster_api_mock():
    with mock.patch('databricks_cli.clusters.cli.ClusterApi') as ClusterApiMock:
        _cluster_api_mock = mock.MagicMock()
        ClusterApiMock.return_value = _cluster_api_mock
        # make sure we always get a cluster name back
        rv = {'cluster_name': TEST_CLUSTER_NAME}
        _cluster_api_mock.get_cluster = mock.MagicMock(return_value=rv)

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


@provide_conf
def test_edit_cli_json_file(cluster_api_mock):
    runner = CliRunner()
    runner.invoke(cli.edit_cli, ['--json-file', 'tests/resources/clusters/edit_cli_input.json'])
    assert cluster_api_mock.edit_cluster.call_args[0][0] == json.loads(EDIT_JSON)


@provide_conf
def test_edit_cli_no_args():
    runner = CliRunner()
    res = runner.invoke(cli.edit_cli, [])
    assert_cli_output(res.output,
                      'Error: RuntimeError: Either --json-file or --json should be provided')


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
def test_resize_cli(cluster_api_mock):
    runner = CliRunner()
    runner.invoke(cli.resize_cli, ['--cluster-id', CLUSTER_ID, '--num-workers', 17])
    assert cluster_api_mock.resize_cluster.call_args[0][0] == CLUSTER_ID
    assert cluster_api_mock.resize_cluster.call_args[0][1] == 17


@provide_conf
def test_delete_cli(cluster_api_mock):
    runner = CliRunner()
    runner.invoke(cli.delete_cli, ['--cluster-id', CLUSTER_ID])
    assert cluster_api_mock.delete_cluster.call_args[0][0] == CLUSTER_ID


@provide_conf
def test_permanent_delete_cli(cluster_api_mock):
    runner = CliRunner()
    runner.invoke(cli.permanent_delete_cli, ['--cluster-id', CLUSTER_ID])
    assert cluster_api_mock.permanent_delete.call_args[0][0] == CLUSTER_ID


@provide_conf
def test_get_cli(cluster_api_mock):
    cluster_api_mock.get_cluster.return_value = '{}'
    runner = CliRunner()
    runner.invoke(cli.get_cli, ['--cluster-id', CLUSTER_ID])
    assert cluster_api_mock.get_cluster.call_args[0][0] == CLUSTER_ID


@pytest.fixture()
def cluster_sdk_mock():
    with mock.patch('databricks_cli.clusters.api.ClusterService') as ClusterSdkMock:
        _cluster_sdk_mock = mock.MagicMock()
        ClusterSdkMock.return_value = _cluster_sdk_mock
        rv = {'cluster_name': TEST_CLUSTER_NAME}
        _cluster_sdk_mock.get_cluster = mock.MagicMock(return_value=rv)

        yield _cluster_sdk_mock


@provide_conf
def test_get_cli_cluster_name(cluster_sdk_mock):
    cluster_sdk_mock.get_clusters_by_name.return_value = CLUSTERS_BY_NAME_SINGLE_CLUSTER_RV
    runner = CliRunner()
    res = runner.invoke(cli.get_cli, ['--cluster-name', CLUSTER_NAME])
    assert_cli_output(res.stdout, '"{}"'.format(TEST_CLUSTER_ID))


@provide_conf
def test_get_cli_cluster_name_multiple(cluster_sdk_mock):
    cluster_sdk_mock.get_clusters_by_name.return_value = CLUSTERS_BY_NAME_MULTIPLE_CLUSTER_RV
    runner = CliRunner()
    res = runner.invoke(cli.get_cli, ['--cluster-name', CLUSTER_NAME])
    assert_cli_output(res.stdout, MULTIPLE_CLUSTERS_FAILURE_OUTPUT)


LIST_RETURN = {
    'clusters': [{
        'cluster_id': TEST_CLUSTER_ID,
        'cluster_name': TEST_CLUSTER_NAME,
        'state': 'PENDING'
    }]
}

EVENTS_RETURN = {
    "events": [
        {
            "cluster_id": TEST_CLUSTER_ID,
            "timestamp": 1559334105421,
            "type": "AUTOSCALING_STATS_REPORT",
            "details": {
                "autoscaling_stats": {
                    "total_instance_seconds": 6530,
                    "unused_instance_seconds": 6530
                }
            }
        },
    ],
    "next_page": {
        "cluster_id": TEST_CLUSTER_ID,
        "end_time": 1562624262942,
        "offset": 50
    },
    "total_count": 87
}


@provide_conf
def test_list_jobs(cluster_api_mock):
    with mock.patch('databricks_cli.clusters.cli.click.echo') as echo_mock:
        cluster_api_mock.list_clusters.return_value = LIST_RETURN
        runner = CliRunner()
        runner.invoke(cli.list_cli)
        assert echo_mock.call_args[0][0] == \
               tabulate([(TEST_CLUSTER_ID, TEST_CLUSTER_NAME, 'PENDING')], tablefmt='plain')


@provide_conf
def test_list_clusters_output_json(cluster_api_mock):
    with mock.patch('databricks_cli.clusters.cli.click.echo') as echo_mock:
        cluster_api_mock.list_clusters.return_value = LIST_RETURN
        runner = CliRunner()
        runner.invoke(cli.list_cli, ['--output', 'json'])
        assert echo_mock.call_args[0][0] == pretty_format(LIST_RETURN)


@provide_conf
def test_cluster_events_output_json(cluster_api_mock):
    with mock.patch('databricks_cli.clusters.cli.click.echo') as echo_mock:
        cluster_api_mock.get_events.return_value = EVENTS_RETURN
        runner = CliRunner()
        runner.invoke(cli.cluster_events_cli, ['--cluster-id', CLUSTER_ID, '--output', 'json'])
        assert echo_mock.call_args[0][0] == pretty_format(EVENTS_RETURN)


@provide_conf
def test_cluster_events_output_table(cluster_api_mock):
    cluster_api_mock.get_events.return_value = EVENTS_RETURN
    runner = CliRunner()
    stdout = runner.invoke(cli.cluster_events_cli, ['--cluster-id', CLUSTER_ID]).stdout
    stdout_lines = stdout.split('\n')
    # Check that the timestamp 1559334105421 gets converted to the right time! It's hard to do an
    # exact match because of time zones.
    assert any(['2019-05-31' in l for l in stdout_lines])  # noqa
