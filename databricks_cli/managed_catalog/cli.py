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

from databricks_cli.click_types import OutputClickType, CatalogNameClickType, SchemaNameClickType
from databricks_cli.configure.config import provide_api_client, profile_option, debug_option
from databricks_cli.managed_catalog.api import ManagedCatalogApi
from databricks_cli.utils import eat_exceptions, CONTEXT_SETTINGS, pretty_format, json_cli_base
from databricks_cli.version import print_version_callback, version


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create table in specified schema.')
@click.option('--catalog', default=None, required=True, type=CatalogNameClickType(),
              help='Catalog containing the parent schema for the new table.')
@click.option('--schema', default=None, required=True, type=SchemaNameClickType(),
              help='Parent schema for the new table.')
@click.option('--output', default=None, help=OutputClickType.help, type=OutputClickType())
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def create_table_cli(api_client, catalog, schema, output):
    """
    Create new table within the specified schema and catalog.

    Calls the 'createTable' RPC endpoint of the Managed Catalog service.
    Returns the properties of the newly-crated table.

    """
    table_json = ManagedCatalogApi(api_client).create_table(catalog, schema)
    click.echo("table_json: %s" % (table_json))
    if OutputClickType.is_json(output):
        click.echo(pretty_format(table_json))
    else:
        click.echo(table_json)

@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Lists tables in specified schema.')
@click.option('--catalog', default=None, required=False, type=CatalogNameClickType(),
              help='List tables within all schemas of specified catalog.')
@click.option('--schema', default=None, required=False, type=SchemaNameClickType(),
              help='List tables within specified schema.')
@click.option('--output', default=None, help=OutputClickType.help, type=OutputClickType())
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def list_tables_cli(api_client, catalog, schema, output):
    """
    Lists tables in specified schema and/or database.

    Calls the 'listTables' RPC endpoint of the Managed Catalog service.
    Returns list of table names for the specified schema and catalog.

    """
    tables_json = ManagedCatalogApi(api_client).list_tables(catalog, schema)
    if OutputClickType.is_json(output):
        click.echo(pretty_format(tables_json))
    else:
        click.echo(tables_json['table_names'])


@click.group(context_settings=CONTEXT_SETTINGS,
             short_help='Utility to interact with Databricks managed-catalog.')
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@debug_option
@profile_option
@eat_exceptions
def managed_catalog_group():  # pragma: no cover
    """
    Utility to interact with Databricks managed-catalog.
    """
    pass

managed_catalog_group.add_command(create_table_cli, name='create-table')
managed_catalog_group.add_command(list_tables_cli, name='list-tables')

