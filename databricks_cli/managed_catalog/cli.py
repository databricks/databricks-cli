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

from databricks_cli.click_types import CatalogNameClickType, \
     SchemaNameClickType, SchemaFullNameClickType, TableFullNameClickType, \
     CommentClickType, DacIdClickType, JsonClickType
from databricks_cli.configure.config import provide_api_client, profile_option, debug_option
from databricks_cli.managed_catalog.api import ManagedCatalogApi
from databricks_cli.utils import eat_exceptions, CONTEXT_SETTINGS, pretty_format, json_cli_base
from databricks_cli.version import print_version_callback, version


#
# Catalog Commands
#
@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create a new catalog.')
@click.option('--name', required=True, type=CatalogNameClickType(),
              help='Name of new catalog.')
@click.option('--comment', default=None, required=False, type=CommentClickType(),
              help='Free-form text description.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def create_catalog_cli(api_client, name, comment):
    """
    Create a new catalog in the specified catalog.

    Calls the 'createCatalog' RPC endpoint of the Managed Catalog service.
    Returns the CatalogInfo for the newly-created catalog.

    """
    catalog_json = ManagedCatalogApi(api_client).create_catalog(name, comment)
    click.echo(pretty_format(catalog_json))

@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Delete a catalog.')
@click.option('--name', required=True, type=CatalogNameClickType(),
              help='Name of the catalog to delete.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def delete_catalog_cli(api_client, name):
    """
    Delete a catalog.

    Calls the 'deleteCatalog' RPC endpoint of the Managed Catalog service.
    Returns nothing.

    """
    ManagedCatalogApi(api_client).delete_catalog(name)

#
# Schema Commands
#
@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create a new schema.')
@click.option('--catalog', required=True, type=CatalogNameClickType(),
              help='Parent catalog of new schema.')
@click.option('--name', required=True, type=SchemaNameClickType(),
              help='Name of new schema, relative to parent catalog.')
@click.option('--comment', default=None, required=False, type=CommentClickType(),
              help='Free-form text description.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def create_schema_cli(api_client, catalog, name, comment):
    """
    Create a new schema in the specified catalog.

    Calls the 'createSchema' RPC endpoint of the Managed Catalog service.
    Returns the SchemaInfo for the newly-created schema.

    """
    schema_json = ManagedCatalogApi(api_client).create_schema(catalog, name, comment)
    click.echo(pretty_format(schema_json))

@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Delete a schema.')
@click.option('--full-name', required=True, type=SchemaFullNameClickType(),
              help='Full name (<catalog>.<schema>) of the schema to delete.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def delete_schema_cli(api_client, full_name):
    """
    Delete a schema.

    Calls the 'deleteSchema' RPC endpoint of the Managed Catalog service.
    Returns nothing.

    """
    ManagedCatalogApi(api_client).delete_schema(full_name)

#
# Table Commands
#
@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create a table.')
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to POST to /api/2.0/managed-catalog/data-access-configurations.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/managed-catalog/data-access-configurations'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def create_table_cli(api_client, json_file, json):
    """
    Create new table specified by the JSON input.

    Calls the 'createTable' RPC endpoint of the Managed Catalog service.
    Returns the properties of the newly-created table.

    """
    json_cli_base(json_file, json,
                  lambda json: ManagedCatalogApi(api_client).create_table(json))

@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Delete a table.')
@click.option('--full-name', required=True, type=TableFullNameClickType(),
              help='Full name (<catalog>.<schema>.<table>) of the table to delete.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def delete_table_cli(api_client, full_name):
    """
    Delete a table.

    Calls the 'deleteTable' RPC endpoint of the Managed Catalog service.
    Returns nothing.

    """
    ManagedCatalogApi(api_client).delete_table(full_name)

#
# Data Access Configuration Commands
#
@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create data access configuration.')
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to POST to /api/2.0/managed-catalog/data-access-configurations.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/managed-catalog/data-access-configurations'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def create_dac_cli(api_client, json_file, json):
    """
    Create new data access configuration.

    Calls the 'createDataAccessConfiguration' RPC endpoint of the Managed Catalog service.
    The specification for the request json can be found at <insert doc link here>.
    Returns the properties of the newly-created DAC.

    """
    json_cli_base(json_file, json,
                  lambda json: ManagedCatalogApi(api_client).create_dac(json))

@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Get data access configuration.')
@click.option('--dac-id', required=True, type=DacIdClickType(),
              help='Data access configuration ID.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def get_dac_cli(api_client, dac_id):
    """
    Get data access configuration details.

    Calls the 'getDataAccessConfiguration' RPC endpoint of the Managed Catalog service.
    Returns details of the DAC specified by its id (TODO: lookup by DAC name?).

    """
    dac_json = ManagedCatalogApi(api_client).get_dac(dac_id)
    click.echo(pretty_format(dac_json))

@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create temporary credentials for storage root access.')
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to POST to /api/2.0/managed-catalog/root-credentials.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/managed-catalog/root-credentials'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def create_root_credentials_cli(api_client, json_file, json):
    """
    Create new temporary credentials (token) for storage root access.

    Calls the 'createRootCredentials' RPC endpoint of the Managed Catalog service.
    The specification for the request json can be found at <insert doc link here>.
    Returns the newly-created temporary credentials.

    """
    json_cli_base(json_file, json,
                  lambda json: ManagedCatalogApi(api_client).create_root_credentials(json))

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
managed_catalog_group.add_command(create_catalog_cli, name='create-catalog')
managed_catalog_group.add_command(delete_catalog_cli, name='delete-catalog')
managed_catalog_group.add_command(create_schema_cli, name='create-schema')
managed_catalog_group.add_command(delete_schema_cli, name='delete-schema')
managed_catalog_group.add_command(create_table_cli, name='create-table')
managed_catalog_group.add_command(delete_table_cli, name='delete-table')
managed_catalog_group.add_command(create_dac_cli, name='create-dac')
managed_catalog_group.add_command(get_dac_cli, name='get-dac')
managed_catalog_group.add_command(create_root_credentials_cli, name='create-root-credentials')
