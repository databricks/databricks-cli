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
               short_help='Create a new schema.')
@click.option('--catalog-name', required=True, help='Parent catalog of new schema.')
@click.option('--name', required=True,
              help='Name of new schema, relative to parent catalog.')
@click.option('--comment', default=None, required=False,
              help='Free-form text description.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def create_schema_cli(api_client, catalog_name, name, comment):
    """
    Create a new schema in the specified catalog.

    Calls the 'createSchema' RPC endpoint of the Unity Catalog service.
    Returns the SchemaInfo for the newly-created schema.

    """
    schema_json = UnityCatalogApi(api_client).create_schema(catalog_name, name, comment)
    click.echo(mc_pretty_format(schema_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='List schemas.')
@click.option('--catalog-name', required=True,
              help='Name of the parent catalog for schemas of interest.')
@click.option('--name-pattern', default=None,
              help='SQL LIKE pattern that the schema name must match to be in list.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def list_schemas_cli(api_client, catalog_name, name_pattern):
    """
    List schemas.

    Calls the 'listSchemas' RPC endpoint of the Unity Catalog service.
    Returns array of SchemaInfos.

    """
    schemas_json = UnityCatalogApi(api_client).list_schemas(catalog_name, name_pattern)
    click.echo(mc_pretty_format(schemas_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Get a schema.')
@click.option('--full-name', required=True,
              help='Full name (<catalog>.<schema>) of the schema to get.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def get_schema_cli(api_client, full_name):
    """
    Get a schema.

    Calls the 'getSchema' RPC endpoint of the Unity Catalog service.
    Returns nothing.

    """
    schema_json = UnityCatalogApi(api_client).get_schema(full_name)
    click.echo(mc_pretty_format(schema_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Update a schema.')
@click.option('--full-name', required=True,
              help='Full name (<catalog>.<schema>) of the schema to update.')
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to PATCH.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/schemas'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def update_schema_cli(api_client, full_name, json_file, json):
    """
    Update a schema.

    Calls the 'updateSchema' RPC endpoint of the Unity Catalog service.
    Returns nothing.

    """
    json_cli_base(json_file, json,
                  lambda json: UnityCatalogApi(api_client).update_schema(full_name, json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Delete a schema.')
@click.option('--full-name', required=True,
              help='Full name (<catalog>.<schema>) of the schema to delete.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def delete_schema_cli(api_client, full_name):
    """
    Delete a schema.

    Calls the 'deleteSchema' RPC endpoint of the Unity Catalog service.
    Returns nothing.

    """
    UnityCatalogApi(api_client).delete_schema(full_name)


def register_schema_commands(cmd_group):
    cmd_group.add_command(create_schema_cli, name='create-schema')
    cmd_group.add_command(list_schemas_cli, name='list-schemas')
    cmd_group.add_command(get_schema_cli, name='get-schema')
    cmd_group.add_command(update_schema_cli, name='update-schema')
    cmd_group.add_command(delete_schema_cli, name='delete-schema')
