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
import click
from tabulate import tabulate
from databricks_cli.click_types import (
    ClusterIdClickType, CommandIdClickType, 
    CommandOutputType, CommandStringType,
    ExecutionContextIdClickType, OutputClickType
)

from databricks_cli.utils import eat_exceptions, CONTEXT_SETTINGS, pretty_format, truncate_string
from databricks_cli.version import print_version_callback, version
from databricks_cli.configure.config import provide_api_client, profile_option, debug_option
from databricks_cli.execution_context.api import ExecutionContext, ExecutionContextApi


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Creates a new execution context.")
@click.option('--cluster-id', required=True, type=ClusterIdClickType(),
              help=ClusterIdClickType.help)
@click.option('--language', required=False, default="python", 
              type=click.Choice(['python', 'scala', 'sql'], case_sensitive=False),
              help="The language for the context.")
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def create_context_cli(api_client, cluster_id, language): #  NOQA
    """
    Creates a new execution context.
    """
    result = ExecutionContextApi(api_client).create_context(cluster_id, language)
    click.echo(pretty_format(result))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Gets information about an execution context.")
@click.option('--cluster-id', required=True, type=ClusterIdClickType(),
              help=ClusterIdClickType.help)
@click.option('--context-id', required=True, type=ExecutionContextIdClickType(),
              help=ExecutionContextIdClickType.help)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def get_context_status_cli(api_client, cluster_id, context_id): #  NOQA
    """
    Gets information about an execution context.
    """
    result = ExecutionContextApi(api_client).get_context_status(cluster_id, context_id)
    click.echo(pretty_format(result))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Deletes an execution context.")
@click.option('--cluster-id', required=True, type=ClusterIdClickType(),
              help=ClusterIdClickType.help)
@click.option('--context-id', required=True, type=ExecutionContextIdClickType(),
              help=ExecutionContextIdClickType.help)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def delete_context_cli(api_client, cluster_id, context_id): #  NOQA
    """
    Deletes an execution context.
    """
    result = ExecutionContextApi(api_client).delete_context(cluster_id, context_id)
    click.echo(pretty_format(result))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Executes a command within an existing execution context.")
@click.option('--output', default=None, help=CommandOutputType.help, type=CommandOutputType())
@click.option('--cluster-id', required=True, type=ClusterIdClickType(),
              help=ClusterIdClickType.help)
@click.option('--context-id', required=True, type=ExecutionContextIdClickType(),
              help=ExecutionContextIdClickType.help)
@click.option('--command', required=True, type=CommandStringType(),
              help=CommandStringType.help)
@click.option('--wait', required=False, default=False, type=click.BOOL,
              help="If true wait for command completion, otherwise schedule "
                   "the command and return immediately.")
@click.option('--language', required=False, default="python", 
              type=click.Choice(['python', 'scala', 'sql'], case_sensitive=False),
              help="The language for the context.")
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def execute_command_cli(api_client, output, cluster_id, context_id, command, language, wait): #  NOQA
    """
    Executes a command within an existing execution context.
    """
    client = ExecutionContextApi(api_client)
    result = client.execute_command(cluster_id, context_id, command, language)
    if not wait:
        click.echo(pretty_format(result))
        return

    command_id = result["id"]
    _wait_for_command(client, output, cluster_id, context_id, command_id)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Executes a single command within a new execution context.")
@click.option('--output', default=None, help=CommandOutputType.help,
              type=CommandOutputType())
@click.option('--cluster-id', required=True, type=ClusterIdClickType(),
              help=ClusterIdClickType.help)
@click.option('--command', required=True, type=CommandStringType(),
              help=CommandStringType.help)
@click.option('--language', required=False, default="python", 
              type=click.Choice(['python', 'scala', 'sql'], case_sensitive=False),
              help="The language for the context.")
@click.option('--wait', required=False, default=False, type=click.BOOL,
              help="If true wait for command completion, otherwise schedule"
                   " the command and return immediately.")
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def execute_command_once_cli(api_client, output, cluster_id, command, language, wait): #  NOQA
    """
    Executes a single command within a new execution context.
    The execution context is automatically created and cleaned up.
    """
    client = ExecutionContextApi(api_client)

    with ExecutionContext(api_client, cluster_id) as context:
        result = client.execute_command(cluster_id, context.id, command, language)
        if not wait:
            click.echo(pretty_format(result))
            return

        command_id = result["id"]
        _wait_for_command(client, output, cluster_id, context.id, command_id)


def _wait_for_command(client, output, cluster_id, context_id, command_id):
    while True:
        status = client.get_command_status(cluster_id, context_id, command_id)
        if not OutputClickType.is_json(output):
            click.echo("Status: " + status["status"])
        if status["status"] in ["Cancelled", "Error", "Finished"]:
            click.echo()
            _print_command_result(output, status)
            break
        time.sleep(1)


def _print_command_result(output, status):
    if OutputClickType.is_json(output):
        click.echo(pretty_format(status))
    else:
        result_type = status["results"]["resultType"]
        data = status["results"]["data"]
        if result_type == "text":
            click.echo("\nCommand ID: %s\n" % status["id"])
            for line in data.splitlines():
                click.echo("output > %s" % line)
        elif result_type == "table":
            # TODO: add CSV output here
            headers = [truncate_string(field["name"], 10) for field in status["results"]["schema"]]
            click.echo(tabulate(tabular_data=data,
                       headers=headers,
                       tablefmt='plain', disable_numparse=True))
        else:
            click.echo(data)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Cancels a command.")
@click.option('--cluster-id', required=True, type=ClusterIdClickType(),
              help=ClusterIdClickType.help)
@click.option('--context-id', required=True, type=ExecutionContextIdClickType(),
              help=ExecutionContextIdClickType.help)
@click.option('--command-id', required=True, type=CommandIdClickType(),
              help=CommandIdClickType.help)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def cancel_command_cli(api_client, cluster_id, context_id, command_id): #  NOQA
    """
    Cancels a command.
    """
    ExecutionContextApi(api_client).cancel_command(cluster_id, context_id, command_id)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Gets information about a command.")
@click.option('--cluster-id', required=True, type=ClusterIdClickType(),
              help=ClusterIdClickType.help)
@click.option('--context-id', required=True, type=ExecutionContextIdClickType(),
              help=ExecutionContextIdClickType.help)
@click.option('--command-id', required=True, type=CommandIdClickType(),
              help=CommandIdClickType.help)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def get_command_status_cli(api_client, cluster_id, context_id, command_id): #  NOQA
    """
    Gets information about a command.
    """
    result = ExecutionContextApi(api_client).get_command_status(cluster_id, context_id, command_id)
    click.echo(pretty_format(result))


@click.group(context_settings=CONTEXT_SETTINGS,
             short_help='Utility to interact with Databricks execution contexts.')
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@debug_option
@profile_option
@eat_exceptions
def execution_context_group():  # pragma: no cover
    """Utility to interact with execution contexts."""
    pass


execution_context_group.add_command(create_context_cli, name='create')
execution_context_group.add_command(get_context_status_cli, name='status')
execution_context_group.add_command(delete_context_cli, name='delete')
execution_context_group.add_command(execute_command_cli, name='command-execute')
execution_context_group.add_command(execute_command_once_cli, name='command-execute-once')
execution_context_group.add_command(cancel_command_cli, name='command-cancel')
execution_context_group.add_command(get_command_status_cli, name='command-status')
