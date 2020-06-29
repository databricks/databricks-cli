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
import time
from datetime import datetime
from json import loads as json_loads

import click
from tabulate import tabulate

from databricks_cli.click_types import OutputClickType, JsonClickType, ClusterPolicyIdClickType
from databricks_cli.clusters_policies.api import ClusterPolicyApi
from databricks_cli.utils import eat_exceptions, CONTEXT_SETTINGS, pretty_format, json_cli_base, \
    truncate_string
from databricks_cli.configure.config import provide_api_client, profile_option, debug_option
from databricks_cli.version import print_version_callback, version


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to POST to /api/2.0/policies/clusters/create.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/policies/clusters/create'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def create_cli(api_client, json_file, json):
    """
    Creates a Databricks cluster Policy.

    The specification for the request json can be found at
    https://docs.databricks.com/dev-tools/api/latest/policies.html#create
    """
    json_cli_base(json_file, json, lambda json: ClusterPolicyApi(api_client).create_cluster_policy(json))


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to POST to /api/2.0/policies/clusters/edit.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/policies/clusters/edit'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def edit_cli(api_client, json_file, json):
    """
    Edits a Databricks cluster Policy.

    The specification for the request json can be found at
    https://docs.databricks.com/dev-tools/api/latest/policies.html#edit
    """
    if not bool(json_file) ^ bool(json):
        raise RuntimeError('Either --json-file or --json should be provided')
    if json_file:
        with open(json_file, 'r') as f:
            json = f.read()
    deser_json = json_loads(json)
    ClusterPolicyApi(api_client).edit_cluster_policy(deser_json)


# @click.command(context_settings=CONTEXT_SETTINGS)
# @click.option('--cluster-id', required=True, type=ClusterIdClickType(),
#               help=ClusterIdClickType.help)
# @click.option('--num-workers', required=True, type=click.INT,
#               help='Number of workers')
# @debug_option
# @profile_option
# @provide_api_client
# @eat_exceptions
# def resize_cli(api_client, cluster_id, num_workers):
#     """Resizes a Databricks cluster given its ID.
#
#     Provide a `--num-workers` parameter to indicate the new cluster size.
#
#     If the cluster is not currently in a RUNNING state, this will cause an
#     error to occur.
#     """
#     ClusterApi(api_client).resize_cluster(cluster_id, num_workers)


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--policy-id', required=True, type=ClusterPolicyIdClickType(),
              help=ClusterPolicyIdClickType.help)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def delete_cli(api_client, policy_id):
    """
    Removes a Databricks cluster policy given its ID.

    Use ``databricks cluster_policiess get --policy-id POLICY_ID`` to check termination states.
    """
    ClusterPolicyApi(api_client).delete_cluster_policy(policy_id)


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--policy-id', required=True, type=ClusterPolicyIdClickType(),
              help=ClusterPolicyIdClickType.help)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def get_cli(api_client, policy_id):
    """
    Retrieves metadata about a cluster policy.
    """
    click.echo(pretty_format(ClusterPolicyApi(api_client).get_cluster_policy(policy_id)))


def _clusters_policies_to_table(policies_json):
    ret = []
    for c in policies_json.get('policies', []):
        ret.append((c['policy_id'], truncate_string(c['policy_name']), c['state']))
    return ret


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Lists Cluster Policiess.')
@click.option('--output', default=None, help=OutputClickType.help, type=OutputClickType())
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def list_cli(api_client, output):
    """
    Lists cluster policiess.

    Returns information about all currently cluster policiess.

    By default the output format will be a human readable table with the following fields

      - Policy ID

      - Policy name

      - Policy state
    """
    policies_json = ClusterPolicyApi(api_client).list_clusters_policies()
    if OutputClickType.is_json(output):
        click.echo(pretty_format(policies_json))
    else:
        click.echo(tabulate(_clusters_policies_to_table(policies_json), tablefmt='plain'))


#
# @click.command(context_settings=CONTEXT_SETTINGS)
# @debug_option
# @profile_option
# @eat_exceptions
# @provide_api_client
# def list_zones_cli(api_client):
#     """
#     Lists zones where clusters can be created.
#
#     The output format is specified in
#     https://docs.databricks.com/api/latest/clusters.html#list-zones
#     """
#     click.echo(pretty_format(ClusterApi(api_client).list_zones()))


# @click.command(context_settings=CONTEXT_SETTINGS,
#                short_help='Lists possible node types for a cluster.')
# @debug_option
# @profile_option
# @eat_exceptions
# @provide_api_client
# def list_node_types_cli(api_client):
#     """
#     Lists possible node types for a cluster.
#
#     The output format is specified in
#     https://docs.databricks.com/api/latest/clusters.html#list-node-types
#     """
#     click.echo(pretty_format(ClusterApi(api_client).list_node_types()))


# @click.command(context_settings=CONTEXT_SETTINGS)
# @debug_option
# @profile_option
# @eat_exceptions
# @provide_api_client
# def spark_versions_cli(api_client):
#     """
#     Lists possible Databricks Runtime versions for a cluster.
#
#     The output format is specified in
#     https://docs.databricks.com/api/latest/clusters.html#spark-versions
#     """
#     click.echo(pretty_format(ClusterApi(api_client).spark_versions()))


@click.group(context_settings=CONTEXT_SETTINGS,
             short_help='Utility to interact with Databricks cluster policies.')
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@debug_option
@profile_option
@eat_exceptions
def clusters_policies_group():
    """
    Utility to interact with Databricks cluster policies.
    """
    pass


clusters_policies_group.add_command(create_cli, name='create')
clusters_policies_group.add_command(edit_cli, name='edit')
# clusters_policies_group.add_command(resize_cli, name='resize')
clusters_policies_group.add_command(delete_cli, name='delete')
clusters_policies_group.add_command(get_cli, name='get')
clusters_policies_group.add_command(list_cli, name='list')
# clusters_policies_group.add_command(list_zones_cli, name='list-zones')
# clusters_policies_group.add_command(list_node_types_cli, name='list-node-types')
# clusters_policies_group.add_command(spark_versions_cli, name='spark-versions')
