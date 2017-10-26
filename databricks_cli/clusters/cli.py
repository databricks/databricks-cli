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

import click
from tabulate import tabulate

from databricks_cli.click_types import OutputClickType
from databricks_cli.clusters.api import create_cluster, start_cluster, restart_cluster, \
    delete_cluster, get_cluster, list_clusters, list_zones, list_node_types, spark_versions
from databricks_cli.utils import eat_exceptions, CONTEXT_SETTINGS, pretty_format, json_cli_base
from databricks_cli.configure.config import require_config
from databricks_cli.version import print_version_callback


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--json-file', default=None,
              help='File containing json to POST to /clusters/create.')
@click.option('--json', default=None, help='Displays absolute paths.')
@require_config
@eat_exceptions
def create_cli(json_file, json):
    """
    Creates a Databricks Cluster

    The specification for the request json can be found at
    https://docs.databricks.com/api/latest/clusters.html#create
    """
    json_cli_base(json_file, json, create_cluster)


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--cluster-id', required=True)
@require_config
@eat_exceptions
def start_cli(cluster_id):
    """
    Starts a terminated Spark cluster given its id.

    If the cluster is not currently in a TERMINATED state, nothing will happen.

    """
    start_cluster(cluster_id)


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--cluster-id', required=True)
@require_config
@eat_exceptions
def restart_cli(cluster_id):
    """
    Restarts a Spark cluster given its id.

    If the cluster is not currently in a RUNNING state, nothing will happen
    """
    restart_cluster(cluster_id)


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--cluster-id', required=True)
@require_config
@eat_exceptions
def delete_cli(cluster_id):
    """
    Removes a Spark cluster given its id.

    The cluster is removed asynchronously. Once the deletion has completed,
    the cluster will be in a TERMINATED state. If the cluster is already in
    a TERMINATING or TERMINATED state, nothing will happen.
    """
    delete_cluster(cluster_id)


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--cluster-id', required=True)
@require_config
@eat_exceptions
def get_cli(cluster_id):
    """
    Retrieves metadata about a cluster.
    """
    click.echo(pretty_format(get_cluster(cluster_id)))


def _clusters_to_table(clusters_json):
    ret = []
    for c in clusters_json.get('clusters', []):
        ret.append((c['cluster_id'], c['cluster_name'], c['state']))
    return ret


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--output', default=None, help=OutputClickType.help, type=OutputClickType())
@require_config
@eat_exceptions
def list_cli(output):
    """
    Lists active and recently terminated clusters.

    Returns information about all currently active clusters, and up
    to 100 most recently terminated clusters in the past 7 days.
    """
    clusters_json = list_clusters()
    if OutputClickType.is_json(output):
        click.echo(pretty_format(clusters_json))
    else:
        click.echo(tabulate(_clusters_to_table(clusters_json), tablefmt='plain'))


@click.command(context_settings=CONTEXT_SETTINGS)
@require_config
@eat_exceptions
def list_zones_cli():
    """
    Lists zones where clusters can be created

    The output format is specified in
    https://docs.databricks.com/api/latest/clusters.html#list-zones
    """
    click.echo(pretty_format(list_zones()))


@click.command(context_settings=CONTEXT_SETTINGS)
@require_config
@eat_exceptions
def list_node_types_cli():
    """
    List possible node types for a cluster.

    The output format is specified in
    https://docs.databricks.com/api/latest/clusters.html#list-node-types
    """
    click.echo(pretty_format(list_node_types()))


@click.command(context_settings=CONTEXT_SETTINGS)
@require_config
@eat_exceptions
def spark_versions_cli():
    """
    List possible spark versions for a cluster.

    The output format is specified in
    https://docs.databricks.com/api/latest/clusters.html#spark-versions
    """
    click.echo(pretty_format(spark_versions()))


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True)
@require_config
@eat_exceptions
def clusters_group():
    """
    Utility to interact with the Clusters service.
    """
    pass


clusters_group.add_command(create_cli, name='create')
clusters_group.add_command(start_cli, name='start')
clusters_group.add_command(restart_cli, name='restart')
clusters_group.add_command(delete_cli, name='delete')
clusters_group.add_command(get_cli, name='get')
clusters_group.add_command(list_cli, name='list')
clusters_group.add_command(list_zones_cli, name='list-zones')
clusters_group.add_command(list_node_types_cli, name='list-node-types')
clusters_group.add_command(spark_versions_cli, name='spark-versions')
