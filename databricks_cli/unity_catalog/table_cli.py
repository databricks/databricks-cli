# Databricks CLI
# Copyright 2021 Databricks, Inc.
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

from databricks_cli.click_types import JsonClickType
from databricks_cli.configure.config import provide_api_client, profile_option, debug_option
from databricks_cli.unity_catalog.api import UnityCatalogApi
from databricks_cli.unity_catalog.utils import mc_pretty_format
from databricks_cli.utils import eat_exceptions, CONTEXT_SETTINGS, json_cli_base


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create a table.')
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to POST.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/unity-catalog/tables'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def create_table_cli(api_client, json_file, json):
    """
    Create new table specified by the JSON input.

    Calls the 'createTable' RPC endpoint of the Unity Catalog service.
    Returns the properties of the newly-created table.

    """
    json_cli_base(json_file, json,
                  lambda json: UnityCatalogApi(api_client).create_table(json),
                  encode_utf8=True)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='List tables.')
@click.option('--catalog-name', required=True,
              help='Name of the parent catalog for tables of interest.')
@click.option('--schema-name', required=True,
              help='Name of the parent schema for tables of interest.')
@click.option('--name-pattern', default=None,
              help='SQL LIKE pattern that the table name must match to be in list.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def list_tables_cli(api_client, catalog_name, schema_name, name_pattern):
    """
    List tables.

    Calls the 'listTables' RPC endpoint of the Unity Catalog service.
    Returns array of TableInfos.

    """
    tables_json = UnityCatalogApi(api_client).list_tables(catalog_name, schema_name, name_pattern)
    click.echo(mc_pretty_format(tables_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='List table summaries.')
@click.option('--catalog-name', required=True,
              help='Name of the parent catalog for tables of interest.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def list_table_summaries_cli(api_client, catalog_name):
    """
    List table summaries (in bulk).

    Calls the 'listTableSummaries' RPC endpoint of the Unity Catalog service.
    Returns array of TableSummarys.

    """
    tables_json = UnityCatalogApi(api_client).list_table_summaries(catalog_name)
    click.echo(mc_pretty_format(tables_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Get a table.')
@click.option('--full-name', required=True,
              help='Full name (<catalog>.<schema>.<table>) of the table to get.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def get_table_cli(api_client, full_name):
    """
    Get a table.

    Calls the 'getTable' RPC endpoint of the Unity Catalog service.
    Returns nothing.

    """
    table_json = UnityCatalogApi(api_client).get_table(full_name)
    click.echo(mc_pretty_format(table_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Update a table.')
@click.option('--full-name', required=True,
              help='Full name (<catalog>.<schema>.<table>) of the table to update.')
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to PATCH.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/tables'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def update_table_cli(api_client, full_name, json_file, json):
    """
    Update a table.

    Calls the 'updateTable' RPC endpoint of the Unity Catalog service.
    Returns nothing.

    """
    json_cli_base(json_file, json,
                  lambda json: UnityCatalogApi(api_client).update_table(full_name, json),
                  encode_utf8=True)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Delete a table.')
@click.option('--full-name', required=True,
              help='Full name (<catalog>.<schema>.<table>) of the table to delete.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def delete_table_cli(api_client, full_name):
    """
    Delete a table.

    Calls the 'deleteTable' RPC endpoint of the Unity Catalog service.
    Returns nothing.

    """
    UnityCatalogApi(api_client).delete_table(full_name)


def register_table_commands(cmd_group):
    cmd_group.add_command(create_table_cli, name='create-table')
    cmd_group.add_command(list_tables_cli, name='list-tables')
    cmd_group.add_command(list_table_summaries_cli, name='list-table-summaries')
    cmd_group.add_command(get_table_cli, name='get-table')
    cmd_group.add_command(update_table_cli, name='update-table')
    cmd_group.add_command(delete_table_cli, name='delete-table')
