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
               short_help='Create a new catalog.')
@click.option('--name', required=True, help='Name of new catalog.')
@click.option('--comment', default=None, required=False,
              help='Free-form text description.')
@click.option('--provider', default=None, required=False,
              help='Name of the Provider (for creating Delta Sharing Catalog).')
@click.option('--share', default=None, required=False,
              help='Name of the Share under the Provider to create a Delta Sharing Catalog.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def create_catalog_cli(api_client, name, comment, provider, share):
    """
    Create a new catalog.

    Calls the 'createCatalog' RPC endpoint of the Unity Catalog service.
    Returns the CatalogInfo for the newly-created catalog.

    """
    catalog_json = UnityCatalogApi(api_client).create_catalog(name, comment,
                                                              provider, share)
    click.echo(mc_pretty_format(catalog_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='List catalogs.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def list_catalogs_cli(api_client):
    """
    List catalogs.

    Calls the 'listCatalogs' RPC endpoint of the Unity Catalog service.
    Returns array of CatalogInfos.

    """
    catalogs_json = UnityCatalogApi(api_client).list_catalogs()
    click.echo(mc_pretty_format(catalogs_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Get a catalog.')
@click.option('--name', required=True,
              help='Name of the catalog to get.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def get_catalog_cli(api_client, name):
    """
    Get a catalog.

    Calls the 'getCatalog' RPC endpoint of the Unity Catalog service.
    Returns nothing.

    """
    catalog_json = UnityCatalogApi(api_client).get_catalog(name)
    click.echo(mc_pretty_format(catalog_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Update a catalog.')
@click.option('--name', required=True,
              help='Name of the catalog to update.')
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to PATCH.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/catalogs'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def update_catalog_cli(api_client, name, json_file, json):
    """
    Update a catalog.

    Calls the 'updateCatalog' RPC endpoint of the Unity Catalog service.
    The public specification for the JSON request is in development.
    Returns nothing.

    """
    json_cli_base(json_file, json,
                  lambda json: UnityCatalogApi(api_client).update_catalog(name, json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Delete a catalog.')
@click.option('--name', required=True,
              help='Name of the catalog to delete.')
@click.option('--purge', '-p', is_flag=True, default=False,
              help='Purge all child schemas and tables of catalog.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def delete_catalog_cli(api_client, name, purge):
    """
    Delete a catalog.

    Calls the 'deleteCatalog' RPC endpoint of the Unity Catalog service.
    Returns nothing.

    """
    if purge:
        tables_response = UnityCatalogApi(api_client).list_table_summaries(name)
        for t in tables_response.get('tables', []):
            click.echo("Deleting table: %s" % (t['full_name']))
            UnityCatalogApi(api_client).delete_table(t['full_name'])

        schemas_response = UnityCatalogApi(api_client).list_schemas(name, None)
        for s in schemas_response.get('schemas', []):
            click.echo("Purging schema: %s" % (s['full_name']))
            UnityCatalogApi(api_client).delete_schema(s['full_name'])

    UnityCatalogApi(api_client).delete_catalog(name)


def register_catalog_commands(cmd_group):
    cmd_group.add_command(create_catalog_cli, name='create-catalog')
    cmd_group.add_command(list_catalogs_cli, name='list-catalogs')
    cmd_group.add_command(get_catalog_cli, name='get-catalog')
    cmd_group.add_command(update_catalog_cli, name='update-catalog')
    cmd_group.add_command(delete_catalog_cli, name='delete-catalog')
