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
from tests.test_data import TEST_CLUSTER_ID, TEST_CLUSTER_NAME, CLUSTER_1_RV
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
def test_get_cli_cluster_name(cluster_sdk_mock):
    cluster_sdk_mock.list_clusters.return_value = LIST_RETURN
    cluster_sdk_mock.get_cluster.return_value = CLUSTER_1_RV
    help_test(cli.get_cli, cluster_sdk_mock.get_cluster, CLUSTER_1_RV,
              ['--cluster-name', CLUSTER_NAME])


@provide_conf
def test_get_cli_cluster_id(cluster_sdk_mock):
    cluster_sdk_mock.list_clusters.return_value = LIST_RETURN
    cluster_sdk_mock.get_cluster.return_value = CLUSTER_1_RV
    help_test(cli.get_cli, cluster_sdk_mock.get_cluster, CLUSTER_1_RV,
              ['--cluster-id', TEST_CLUSTER_ID])


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


def help_test(cli_function, service_function, rv, args=None):
    """
    This function makes testing the cli functions that just pass data through simpler
    """

    if args is None:
        args = []

    with mock.patch('databricks_cli.clusters.cli.click.echo') as echo_mock:
        service_function.return_value = rv
        runner = CliRunner()
        runner.invoke(cli_function, args)
        assert echo_mock.call_args[0][0] == pretty_format(rv)


@provide_conf
def test_list_zones(cluster_sdk_mock):
    zones_rv = {
        'zones': [
            'us-west-2a',
            'us-west-2b',
            'us-west-2c',
            'us-west-2d'
        ],
        'default_zone': 'us-west-2a'
    }

    help_test(cli.list_zones_cli, cluster_sdk_mock.list_available_zones, zones_rv)


@provide_conf
def test_list_node_types(cluster_sdk_mock):
    rv = {
        "node_types": [
            {
                "node_type_id": "r3.xlarge",
                "memory_mb": 31232,
                "num_cores": 4.0,
                "description": "r3.xlarge (deprecated)",
                "instance_type_id": "r3.xlarge",
                "is_deprecated": False,
                "category": "Memory Optimized",
                "support_ebs_volumes": True,
                "support_cluster_tags": True,
                "num_gpus": 0,
                "node_instance_type": {
                    "instance_type_id": "r3.xlarge",
                    "local_disks": 1,
                    "local_disk_size_gb": 80
                },
                "is_hidden": False,
                "support_port_forwarding": True,
                "display_order": 1,
                "is_io_cache_enabled": False
            }
        ]
    }

    help_test(cli.list_node_types_cli, cluster_sdk_mock.list_node_types, rv)


@provide_conf
def test_spark_versions(cluster_sdk_mock):
    rv = {
        "versions": [
            {
                "key": "7.1.x-scala2.12",
                "name": "7.1 (includes Apache Spark 3.0.0, Scala 2.12)"
            },
            {
                "key": "6.5.x-scala2.11",
                "name": "6.5 (includes Apache Spark 2.4.5, Scala 2.11)"
            }
        ]
    }

    help_test(cli.spark_versions_cli, cluster_sdk_mock.list_spark_versions, rv)
