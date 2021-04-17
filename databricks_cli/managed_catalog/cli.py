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

from databricks_cli.click_types import MetastoreIdClickType, DacIdClickType, \
    JsonClickType, OneOfOption
from databricks_cli.configure.config import provide_api_client, profile_option, debug_option
from databricks_cli.managed_catalog.api import ManagedCatalogApi
from databricks_cli.utils import eat_exceptions, CONTEXT_SETTINGS, pretty_format, json_cli_base
from databricks_cli.version import print_version_callback, version


#################  Metastore Commands  #####################


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create a metastore.')
@click.option('--name', required=True, help='Name of the new metastore.')
@click.option('--storage-root', required=True,
              help='Storage root URL for the new metastore.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def create_metastore_cli(api_client, name, storage_root):
    """
    Create new metastore specified by the JSON input.

    Calls the 'createMetastore' RPC endpoint of the Managed Catalog service.
    Returns the properties of the newly-created metastore.

    """
    metastore_json = ManagedCatalogApi(api_client).create_metastore(name, storage_root)
    click.echo(pretty_format(metastore_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='List metastores.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def list_metastores_cli(api_client):
    """
    List metastores.

    Calls the 'listMetastores' RPC endpoint of the Managed Catalog service.
    Returns array of MetastoreInfos.

    """
    metastores_json = ManagedCatalogApi(api_client).list_metastores()
    click.echo(pretty_format(metastores_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Get a metastore.')
@click.option('--id', 'metastore_id', required=True, type=MetastoreIdClickType(),
              help='Unique identifier of the metastore to get.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def get_metastore_cli(api_client, metastore_id):
    """
    Get a metastore.

    Calls the 'getMetastore' RPC endpoint of the Managed Catalog service.
    Returns nothing.

    """
    metastore_json = ManagedCatalogApi(api_client).get_metastore(metastore_id)
    click.echo(pretty_format(metastore_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Update a metastore.')
@click.option('--id', 'metastore_id', required=True, type=MetastoreIdClickType(),
              help='Unique identifier of the metastore to update.')
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to PATCH.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/admin/metastores'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def update_metastore_cli(api_client, metastore_id, json_file, json):
    """
    Update a metastore.

    Calls the 'updateMetastore' RPC endpoint of the Managed Catalog service.
    Returns nothing.

    """
    json_cli_base(json_file, json,
                  lambda json: ManagedCatalogApi(api_client).update_metastore(metastore_id, json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Delete a metastore.')
@click.option('--id', 'metastore_id', required=True, type=MetastoreIdClickType(),
              help='Unique identifier of the metastore to delete.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def delete_metastore_cli(api_client, metastore_id):
    """
    Delete a metastore.

    Calls the 'deleteMetastore' RPC endpoint of the Managed Catalog service.
    Returns nothing.

    """
    ManagedCatalogApi(api_client).delete_metastore(metastore_id)


##############  Catalog Commands  ##############


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create a new catalog.')
@click.option('--name', required=True, help='Name of new catalog.')
@click.option('--comment', default=None, required=False,
              help='Free-form text description.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def create_catalog_cli(api_client, name, comment):
    """
    Create a new catalog.

    Calls the 'createCatalog' RPC endpoint of the Managed Catalog service.
    Returns the CatalogInfo for the newly-created catalog.

    """
    catalog_json = ManagedCatalogApi(api_client).create_catalog(name, comment)
    click.echo(pretty_format(catalog_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='List catalogs.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def list_catalogs_cli(api_client):
    """
    List catalogs.

    Calls the 'listCatalogs' RPC endpoint of the Managed Catalog service.
    Returns array of CatalogInfos.

    """
    catalogs_json = ManagedCatalogApi(api_client).list_catalogs()
    click.echo(pretty_format(catalogs_json))


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

    Calls the 'getCatalog' RPC endpoint of the Managed Catalog service.
    Returns nothing.

    """
    catalog_json = ManagedCatalogApi(api_client).get_catalog(name)
    click.echo(pretty_format(catalog_json))


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

    Calls the 'updateCatalog' RPC endpoint of the Managed Catalog service.
    Returns nothing.

    """
    json_cli_base(json_file, json,
                  lambda json: ManagedCatalogApi(api_client).update_catalog(name, json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Delete a catalog.')
@click.option('--name', required=True,
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


#############  Schema Commands ##############


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create a new schema.')
@click.option('--catalog', required=True, help='Parent catalog of new schema.')
@click.option('--name', required=True,
              help='Name of new schema, relative to parent catalog.')
@click.option('--comment', default=None, required=False,
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
               short_help='List schemas.')
@click.option('--catalog-name', required=True,
              help='Name of the parent catalog for schemas of interest.')
@click.option('--name-regex', default=None,
              help='Regex that the schema name must match to be in list.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def list_schemas_cli(api_client, catalog_name, name_regex):
    """
    List schemas.

    Calls the 'listSchemas' RPC endpoint of the Managed Catalog service.
    Returns array of SchemaInfos.

    """
    schemas_json = ManagedCatalogApi(api_client).list_schemas(catalog_name, name_regex)
    click.echo(pretty_format(schemas_json))


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

    Calls the 'getSchema' RPC endpoint of the Managed Catalog service.
    Returns nothing.

    """
    schema_json = ManagedCatalogApi(api_client).get_schema(full_name)
    click.echo(pretty_format(schema_json))


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

    Calls the 'updateSchema' RPC endpoint of the Managed Catalog service.
    Returns nothing.

    """
    json_cli_base(json_file, json,
                  lambda json: ManagedCatalogApi(api_client).update_schema(full_name, json))


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

    Calls the 'deleteSchema' RPC endpoint of the Managed Catalog service.
    Returns nothing.

    """
    ManagedCatalogApi(api_client).delete_schema(full_name)


##############  Table Commands  #################


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create a table.')
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to POST.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/managed-catalog/tables'))
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
               short_help='List tables.')
@click.option('--catalog-name', required=True,
              help='Name of the parent catalog for tables of interest.')
@click.option('--schema-name', required=True,
              help='Name of the parent schema for tables of interest.')
@click.option('--name-regex', default=None,
              help='Regex that the table name must match to be in list.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def list_tables_cli(api_client, catalog_name, schema_name, name_regex):
    """
    List tables.

    Calls the 'listTables' RPC endpoint of the Managed Catalog service.
    Returns array of TableInfos.

    """
    tables_json = ManagedCatalogApi(api_client).list_tables(catalog_name, schema_name, name_regex)
    click.echo(pretty_format(tables_json))


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

    Calls the 'getTable' RPC endpoint of the Managed Catalog service.
    Returns nothing.

    """
    table_json = ManagedCatalogApi(api_client).get_table(full_name)
    click.echo(pretty_format(table_json))


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

    Calls the 'updateTable' RPC endpoint of the Managed Catalog service.
    Returns nothing.

    """
    json_cli_base(json_file, json,
                  lambda json: ManagedCatalogApi(api_client).update_table(full_name, json))


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

    Calls the 'deleteTable' RPC endpoint of the Managed Catalog service.
    Returns nothing.

    """
    ManagedCatalogApi(api_client).delete_table(full_name)


#############  Data Access Configuration Commands  ############


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create data access configuration.')
@click.option('--metastore-id', required=True, type=MetastoreIdClickType(),
              help='Unique identifier of the metastore parent of the DAC.')
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to POST.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/managed-catalog/data-access-configurations'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def create_dac_cli(api_client, metastore_id, json_file, json):
    """
    Create new data access configuration.

    Calls the 'createDataAccessConfiguration' RPC endpoint of the Managed Catalog service.
    The specification for the request json can be found at <insert doc link here>.
    Returns the properties of the newly-created DAC.

    """
    json_cli_base(json_file, json,
                  lambda json: ManagedCatalogApi(api_client).create_dac(metastore_id, json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='List data access configurations.')
@click.option('--metastore-id', required=True, type=MetastoreIdClickType(),
              help='Unique identifier of the metastore parent of the DAC(s).')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def list_dacs_cli(api_client, metastore_id):
    """
    List data access configurations.

    Calls the 'listDataAccessConfigurations' RPC endpoint of the Managed Catalog service.
    Returns array of DataAccessConfigurations.

    """
    dacs_json = ManagedCatalogApi(api_client).list_dacs(metastore_id)
    click.echo(pretty_format(dacs_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Get data access configuration.')
@click.option('--metastore-id', required=True, type=MetastoreIdClickType(),
              help='Unique identifier of the metastore parent of the DAC.')
@click.option('--dac-id', required=True, type=DacIdClickType(),
              help='Data access configuration ID.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def get_dac_cli(api_client, metastore_id, dac_id):
    """
    Get data access configuration details.

    Calls the 'getDataAccessConfiguration' RPC endpoint of the Managed Catalog service.
    Returns details of the DAC specified by its id (TODO: lookup by DAC name?).

    """
    dac_json = ManagedCatalogApi(api_client).get_dac(metastore_id, dac_id)
    click.echo(pretty_format(dac_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Delete data access configuration.')
@click.option('--metastore-id', required=True, type=MetastoreIdClickType(),
              help='Unique identifier of the metastore parent of the DAC.')
@click.option('--dac-id', required=True, type=DacIdClickType(),
              help='Data access configuration ID.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def delete_dac_cli(api_client, metastore_id, dac_id):
    """
    Delete data access configuration details.

    Calls the 'deleteDataAccessConfiguration' RPC endpoint of the Managed Catalog service.

    """
    ManagedCatalogApi(api_client).delete_dac(metastore_id, dac_id)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create temporary credentials for storage root access.')
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to POST.')
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


PERMISSIONS_OBJ_TYPES = ['catalog', 'schema', 'table']


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Get permissions on a securable.')
@click.option('--catalog', cls=OneOfOption, default=None,
              one_of=PERMISSIONS_OBJ_TYPES,
              help='Name of catalog of interest')
@click.option('--schema', cls=OneOfOption, default=None,
              one_of=PERMISSIONS_OBJ_TYPES,
              help='Full name of schema of interest')
@click.option('--table', cls=OneOfOption, default=None,
              one_of=PERMISSIONS_OBJ_TYPES,
              help='Full name of table of interest')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def get_permissions_cli(api_client, catalog, schema, table):
    """
    Get permissions on a securable.

    Calls the 'getPermissions' RPC endpoint of the Managed Catalog service.
    Returns PermissionsList for the requested securable.

    """
    perm_json = ManagedCatalogApi(api_client).get_permissions(catalog, schema, table)
    click.echo(pretty_format(perm_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='update permissions on a securable.')
@click.option('--catalog', cls=OneOfOption, default=None,
              one_of=PERMISSIONS_OBJ_TYPES,
              help='Name of catalog of interest')
@click.option('--schema', cls=OneOfOption, default=None,
              one_of=PERMISSIONS_OBJ_TYPES,
              help='Full name of schema of interest')
@click.option('--table', cls=OneOfOption, default=None,
              one_of=PERMISSIONS_OBJ_TYPES,
              help='Full name of table of interest')
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON of permissions change to PATCH.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/managed-catalog/permissions'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def update_permissions_cli(api_client, catalog, schema, table, json_file, json):
    """
    Update permissions on a securable.

    Calls the 'updatePermissions' RPC endpoint of the Managed Catalog service.
    Returns updated PermissionsList for the requested securable.

    """
    json_cli_base(json_file, json,
                  lambda json: ManagedCatalogApi(api_client).update_permissions(catalog, schema,
                                                                                table, json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='replace permissions on a securable.')
@click.option('--catalog', cls=OneOfOption, default=None,
              one_of=PERMISSIONS_OBJ_TYPES,
              help='Name of catalog of interest')
@click.option('--schema', cls=OneOfOption, default=None,
              one_of=PERMISSIONS_OBJ_TYPES,
              help='Full name of schema of interest')
@click.option('--table', cls=OneOfOption, default=None,
              one_of=PERMISSIONS_OBJ_TYPES,
              help='Full name of table of interest')
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON of permissions to PUT.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/managed-catalog/permissions'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def replace_permissions_cli(api_client, catalog, schema, table, json_file, json):
    """
    Replace permissions on a securable.

    Calls the 'replacePermissions' RPC endpoint of the Managed Catalog service.
    Returns nothing.

    """
    json_cli_base(json_file, json,
                  lambda json: ManagedCatalogApi(api_client).replace_permissions(catalog, schema,
                                                                                 table, json))


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


managed_catalog_group.add_command(create_metastore_cli, name='create-metastore')
managed_catalog_group.add_command(list_metastores_cli, name='list-metastores')
managed_catalog_group.add_command(get_metastore_cli, name='get-metastore')
managed_catalog_group.add_command(update_metastore_cli, name='update-metastore')
managed_catalog_group.add_command(delete_metastore_cli, name='delete-metastore')
managed_catalog_group.add_command(create_catalog_cli, name='create-catalog')
managed_catalog_group.add_command(list_catalogs_cli, name='list-catalogs')
managed_catalog_group.add_command(get_catalog_cli, name='get-catalog')
managed_catalog_group.add_command(update_catalog_cli, name='update-catalog')
managed_catalog_group.add_command(delete_catalog_cli, name='delete-catalog')
managed_catalog_group.add_command(create_schema_cli, name='create-schema')
managed_catalog_group.add_command(list_schemas_cli, name='list-schemas')
managed_catalog_group.add_command(get_schema_cli, name='get-schema')
managed_catalog_group.add_command(update_schema_cli, name='update-schema')
managed_catalog_group.add_command(delete_schema_cli, name='delete-schema')
managed_catalog_group.add_command(create_table_cli, name='create-table')
managed_catalog_group.add_command(list_tables_cli, name='list-tables')
managed_catalog_group.add_command(get_table_cli, name='get-table')
managed_catalog_group.add_command(update_table_cli, name='update-table')
managed_catalog_group.add_command(delete_table_cli, name='delete-table')
managed_catalog_group.add_command(create_dac_cli, name='create-dac')
managed_catalog_group.add_command(list_dacs_cli, name='list-dacs')
managed_catalog_group.add_command(get_dac_cli, name='get-dac')
managed_catalog_group.add_command(delete_dac_cli, name='delete-dac')
managed_catalog_group.add_command(get_permissions_cli, name='get-permissions')
managed_catalog_group.add_command(update_permissions_cli, name='update-permissions')
managed_catalog_group.add_command(replace_permissions_cli, name='replace-permissions')
managed_catalog_group.add_command(create_root_credentials_cli, name='create-root-credentials')
