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
    WorkspaceIdClickType, JsonClickType, OneOfOption
from databricks_cli.configure.config import provide_api_client, profile_option, debug_option
from databricks_cli.unity_catalog.api import UnityCatalogApi
from databricks_cli.utils import eat_exceptions, CONTEXT_SETTINGS, pretty_format, json_cli_base
from databricks_cli.version import print_version_callback, version


# Encode UTF-8 strings in JSON blobs
def mc_pretty_format(json):
    return pretty_format(json, encode_utf8=True)


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

    Calls the 'createMetastore' RPC endpoint of the Unity Catalog service.
    Returns the properties of the newly-created metastore.

    """
    metastore_json = UnityCatalogApi(api_client).create_metastore(name, storage_root)
    click.echo(mc_pretty_format(metastore_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='List metastores.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def list_metastores_cli(api_client):
    """
    List metastores.

    Calls the 'listMetastores' RPC endpoint of the Unity Catalog service.
    Returns array of MetastoreInfos.

    """
    metastores_json = UnityCatalogApi(api_client).list_metastores()
    click.echo(mc_pretty_format(metastores_json))


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

    Calls the 'getMetastore' RPC endpoint of the Unity Catalog service.
    Returns nothing.

    """
    metastore_json = UnityCatalogApi(api_client).get_metastore(metastore_id)
    click.echo(mc_pretty_format(metastore_json))


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

    Calls the 'updateMetastore' RPC endpoint of the Unity Catalog service.
    Returns nothing.

    """
    json_cli_base(json_file, json,
                  lambda json: UnityCatalogApi(api_client).update_metastore(metastore_id, json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Delete a metastore.')
@click.option('--id', 'metastore_id', required=True, type=MetastoreIdClickType(),
              help='Unique identifier of the metastore to delete.')
@click.option('--force', '-f', is_flag=True, default=False)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def delete_metastore_cli(api_client, metastore_id, force):
    """
    Delete a metastore.

    Calls the 'deleteMetastore' RPC endpoint of the Unity Catalog service.
    Returns nothing.

    """
    UnityCatalogApi(api_client).delete_metastore(metastore_id, force)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Get summary info of current metastore.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def metastore_summary_cli(api_client):
    """
    Get metastore summary.

    Calls the 'getMetastoreSummary' RPC endpoint of the Unity Catalog service.
    Returns nothing.

    """
    summary_json = UnityCatalogApi(api_client).get_metastore_summary()
    click.echo(mc_pretty_format(summary_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Assign a metastore to the current workspace.')
@click.option('--workspace-id', 'workspace_id', required=True, type=WorkspaceIdClickType(),
              help='Unique identifier of the metastore to assign.')
@click.option('--metastore-id', 'metastore_id', required=True, type=MetastoreIdClickType(),
              help='Unique identifier of the metastore to assign.')
@click.option('--default-catalog-name', 'default_catalog_name', required=False, default='main',
              help='Name of the default catalog to use with the metastore.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def assign_metastore_cli(api_client, workspace_id, metastore_id, default_catalog_name):
    """
    Assign a metastore to the current workspace.

    Calls the 'createMetastoreAssignment' RPC endpoint of the Unity Catalog service.
    If that fails due to the workspace already having a Metastore assigned, it calls
    the 'updateMetastoreAssignment' endpoint.
    Returns nothing.

    """
    resp = UnityCatalogApi(api_client).create_metastore_assignment(workspace_id, metastore_id,
                                                                   default_catalog_name)
    # resp will just be an empty object ('{}') but it's good to print *something*
    click.echo(mc_pretty_format(resp))


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

    Calls the 'createCatalog' RPC endpoint of the Unity Catalog service.
    Returns the CatalogInfo for the newly-created catalog.

    """
    catalog_json = UnityCatalogApi(api_client).create_catalog(name, comment)
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
    Returns nothing.

    """
    json_cli_base(json_file, json,
                  lambda json: UnityCatalogApi(api_client).update_catalog(name, json))


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

    Calls the 'deleteCatalog' RPC endpoint of the Unity Catalog service.
    Returns nothing.

    """
    UnityCatalogApi(api_client).delete_catalog(name)


#############  Schema Commands ##############


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


##############  Table Commands  #################


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
               short_help='List tables bulk.')
@click.option('--catalog-name', required=True,
              help='Name of the parent catalog for tables of interest.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def list_tables_bulk_cli(api_client, catalog_name):
    """
    List tables bulk.

    Calls the 'listTablesBulk' RPC endpoint of the Unity Catalog service.
    Returns array of TableSummarys.

    """
    tables_json = UnityCatalogApi(api_client).list_tables_bulk(catalog_name)
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


#############  Data Access Configuration Commands  ############


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create data access configuration.')
@click.option('--metastore-id', required=True, type=MetastoreIdClickType(),
              help='Unique identifier of the metastore parent of the DAC.')
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to POST.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/unity-catalog/data-access-configurations'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def create_dac_cli(api_client, metastore_id, json_file, json):
    """
    Create new data access configuration.

    Calls the 'createDataAccessConfiguration' RPC endpoint of the Unity Catalog service.
    The specification for the request json can be found at <insert doc link here>.
    Returns the properties of the newly-created DAC.

    """
    json_cli_base(json_file, json,
                  lambda json: UnityCatalogApi(api_client).create_dac(metastore_id, json),
                  encode_utf8=True)


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

    Calls the 'listDataAccessConfigurations' RPC endpoint of the Unity Catalog service.
    Returns array of DataAccessConfigurations.

    """
    dacs_json = UnityCatalogApi(api_client).list_dacs(metastore_id)
    click.echo(mc_pretty_format(dacs_json))


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

    Calls the 'getDataAccessConfiguration' RPC endpoint of the Unity Catalog service.
    Returns details of the DAC specified by its id (TODO: lookup by DAC name?).

    """
    dac_json = UnityCatalogApi(api_client).get_dac(metastore_id, dac_id)
    click.echo(mc_pretty_format(dac_json))


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

    Calls the 'deleteDataAccessConfiguration' RPC endpoint of the Unity Catalog service.

    """
    UnityCatalogApi(api_client).delete_dac(metastore_id, dac_id)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create temporary credentials for storage root access.')
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to POST.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/unity-catalog/root-credentials'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def create_root_credentials_cli(api_client, json_file, json):
    """
    Create new temporary credentials (token) for storage root access.

    Calls the 'createRootCredentials' RPC endpoint of the Unity Catalog service.
    The specification for the request json can be found at <insert doc link here>.
    Returns the newly-created temporary credentials.

    """
    json_cli_base(json_file, json,
                  lambda json: UnityCatalogApi(api_client).create_root_credentials(json),
                  encode_utf8=True)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create storage credential.')
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to POST.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/unity-catalog/storage-credentials'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def create_credential_cli(api_client, json_file, json):
    """
    Create new storage credential.

    Calls the 'createStorageCredential' RPC endpoint of the Unity Catalog service.
    The specification for the request json can be found at <insert doc link here>.
    Returns the properties of the newly-created Storage Credential.

    """
    json_cli_base(json_file, json,
                  lambda json: UnityCatalogApi(api_client).create_storage_credential(json),
                  encode_utf8=True)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='List storage credentials.')
@click.option('--name-pattern', default=None,
              help='SQL LIKE pattern that the credential name must match to be in list.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def list_credentials_cli(api_client, name_pattern):
    """
    List storage credentials.

    Calls the 'listStorageCredentials' RPC endpoint of the Unity Catalog service.
    Returns array of StorageCredentials.

    """
    creds_json = UnityCatalogApi(api_client).list_storage_credentials(name_pattern)
    click.echo(mc_pretty_format(creds_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Get a storage credential.')
@click.option('--name', required=True,
              help='Name of the storage credential to get.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def get_credential_cli(api_client, name):
    """
    Get a storage credential.

    Calls the 'getStorageCredential' RPC endpoint of the Unity Catalog service.
    Returns an StorageCredential object.

    """
    cred_json = UnityCatalogApi(api_client).get_storage_credential(name)
    click.echo(mc_pretty_format(cred_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Update a storage credential.')
@click.option('--name', required=True,
              help='Name of the storage credential to update.')
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to PATCH.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/storage-credentials'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def update_credential_cli(api_client, name, json_file, json):
    """
    Update a storage credential.

    Calls the 'updateStorageCredential' RPC endpoint of the Unity Catalog service.
    Returns nothing.

    """
    json_cli_base(json_file, json,
                  lambda json: UnityCatalogApi(api_client).update_storage_credential(name, json),
                  encode_utf8=True)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Delete a storage credential.')
@click.option('--name', required=True,
              help='Name of the storage credential to delete.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def delete_credential_cli(api_client, name):
    """
    Delete a storage credential.

    Calls the 'deleteStorageCredential' RPC endpoint of the Unity Catalog service.
    Returns nothing.

    """
    UnityCatalogApi(api_client).delete_storage_credential(name)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create External Location.')
@click.option('--name', default=None,
              help='Name of new external location')
@click.option('--url', default=None,
              help='Path URL for the new external location')
@click.option('--storage-credential-name', default=None,
              help='Name of storage credential to use with new external location')
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to POST.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/unity-catalog/external-locations'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def create_location_cli(api_client, name, url, storage_credential_name, json_file, json):
    """
    Create new external location.

    Calls the 'createExternalLocation' RPC endpoint of the Unity Catalog service.
    The specification for the request json can be found at <insert doc link here>.
    Returns the properties of the newly-created Storage Credential.

    """
    if (name is not None) and (url is not None) and (storage_credential_name is not None):
        if (json_file is not None) or (json is not None):
            raise ValueError('Cannot specify JSON if both name and url are given')
        data = {"name": name, "url": url, "credential_name": storage_credential_name}
        loc_json = UnityCatalogApi(api_client).create_external_location(data)
        click.echo(mc_pretty_format(loc_json))
    elif (json is None) and (json_file is None):
        raise ValueError('Must provide name, url and storage-credential-name or use JSON specification')
    else:
        json_cli_base(json_file, json,
                      lambda json: UnityCatalogApi(api_client).create_external_location(json),
                      encode_utf8=True)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='List external locations.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def list_locations_cli(api_client, ):
    """
    List external locations.

    Calls the 'listExternalLocations' RPC endpoint of the Unity Catalog service.
    Returns array of ExternalLocations.

    """
    locs_json = UnityCatalogApi(api_client).list_external_locations()
    click.echo(mc_pretty_format(locs_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Get an external location.')
@click.option('--name', required=True,
              help='Name of the external location to get.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def get_location_cli(api_client, name):
    """
    Get an external location.

    Calls the 'getExternalLocation' RPC endpoint of the Unity Catalog service.
    Returns an ExternalLocation object.

    """
    loc_json = UnityCatalogApi(api_client).get_external_location(name)
    click.echo(mc_pretty_format(loc_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Update an external location.')
@click.option('--name', required=True,
              help='Name of the external location to update.')
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to PATCH.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/external-locations'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def update_location_cli(api_client, name, json_file, json):
    """
    Update an external location.

    Calls the 'updateExternalLocation' RPC endpoint of the Unity Catalog service.
    Returns nothing.

    """
    json_cli_base(json_file, json,
                  lambda json: UnityCatalogApi(api_client).update_external_location(name, json),
                  encode_utf8=True)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Delete an external location.')
@click.option('--name', required=True,
              help='Name of the external location to delete.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def delete_location_cli(api_client, name):
    """
    Delete an external location.

    Calls the 'deleteExternalLocation' RPC endpoint of the Unity Catalog service.
    Returns nothing.

    """
    UnityCatalogApi(api_client).delete_external_location(name)


PERMISSIONS_OBJ_TYPES = [
    'catalog', 'schema', 'table', 'share', 'storage-credential', 'external-location'
]


def _get_perm_securable_name_and_type(catalog_name, schema_full_name, table_full_name,
                                      share_name, credential_name, location_name):
    if catalog_name:
        return ('catalog', catalog_name)
    elif schema_full_name:
        return ('schema', schema_full_name)
    elif table_full_name:
        return ('table', table_full_name)
    elif share_name:
        return ('share', share_name)
    elif credential_name:
        return ('storage-credential', credential_name)
    else:
        return ('external-location', location_name)


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
@click.option('--share', cls=OneOfOption, default=None,
              one_of=PERMISSIONS_OBJ_TYPES,
              help='Name of the share of interest')
@click.option('--storage-credential', cls=OneOfOption, default=None,
              one_of=PERMISSIONS_OBJ_TYPES,
              help='Name of the storage credential of interest')
@click.option('--external-location', cls=OneOfOption, default=None,
              one_of=PERMISSIONS_OBJ_TYPES,
              help='Name of the external location of interest')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def get_permissions_cli(api_client, catalog, schema, table, share, storage_credential,
                        external_location):
    """
    Get permissions on a securable.

    Calls the 'getPermissions' RPC endpoint of the Unity Catalog service.
    Returns PermissionsList for the requested securable.

    """
    sec_type, sec_name = _get_perm_securable_name_and_type(catalog, schema, table, share,
                                                           storage_credential, external_location)

    perm_json = UnityCatalogApi(api_client).get_permissions(sec_type, sec_name)
    click.echo(mc_pretty_format(perm_json))


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
@click.option('--share', cls=OneOfOption, default=None,
              one_of=PERMISSIONS_OBJ_TYPES,
              help='Name of the share of interest')
@click.option('--storage-credential', cls=OneOfOption, default=None,
              one_of=PERMISSIONS_OBJ_TYPES,
              help='Name of the storage credential of interest')
@click.option('--external-location', cls=OneOfOption, default=None,
              one_of=PERMISSIONS_OBJ_TYPES,
              help='Name of the external location of interest')
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON of permissions change to PATCH.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/unity-catalog/permissions'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def update_permissions_cli(api_client, catalog, schema, table, share, storage_credential,
                           external_location, json_file, json):
    """
    Update permissions on a securable.

    Calls the 'updatePermissions' RPC endpoint of the Unity Catalog service.
    Returns updated PermissionsList for the requested securable.

    """
    sec_type, sec_name = _get_perm_securable_name_and_type(catalog, schema, table, share,
                                                           storage_credential, external_location)

    json_cli_base(json_file, json,
                  lambda json: UnityCatalogApi(api_client).update_permissions(sec_type, sec_name,
                                                                              json),
                  encode_utf8=True)


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
# shares not supported for replace permissions
@click.option('--credential', cls=OneOfOption, default=None,
              one_of=PERMISSIONS_OBJ_TYPES,
              help='Name of the storage credential of interest')
@click.option('--location', cls=OneOfOption, default=None,
              one_of=PERMISSIONS_OBJ_TYPES,
              help='Name of the external location of interest')
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON of permissions to PUT.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/unity-catalog/permissions'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def replace_permissions_cli(api_client, catalog, schema, table, credential, location,
                            json_file, json):
    """
    Replace permissions on a securable.

    Calls the 'replacePermissions' RPC endpoint of the Unity Catalog service.
    Returns nothing.

    """
    share = None  # shares not supported for replace permissions

    sec_type, sec_name = _get_perm_securable_name_and_type(catalog, schema, table, share,
                                                           credential, location)
    json_cli_base(json_file, json,
                  lambda json: UnityCatalogApi(api_client).replace_permissions(sec_type, sec_name,
                                                                               json),
                  encode_utf8=True)

##############  Share Commands  ##############


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create a new share.')
@click.option('--name', required=True, help='Name of new share.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def create_share_cli(api_client, name):
    """
    Create a new share.

    Calls the 'createShare' RPC endpoint of the Unity Catalog service.
    Returns the ShareInfo for the newly-created share.

    """
    share_json = UnityCatalogApi(api_client).create_share(name)
    click.echo(mc_pretty_format(share_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='List shares.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def list_shares_cli(api_client):
    """
    List shares.

    Calls the 'listShares' RPC endpoint of the Unity Catalog service.
    Returns array of ShareInfos.

    """
    shares_json = UnityCatalogApi(api_client).list_shares()
    click.echo(mc_pretty_format(shares_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Get a share.')
@click.option('--name', required=True,
              help='Name of the share to get.')
@click.option('--include_shared_data', default=True,
              help='Whether to include shared data in the response.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def get_share_cli(api_client, name, include_shared_data):
    """
    Get a share.

    Calls the 'getShare' RPC endpoint of the Unity Catalog service.
    Returns nothing.

    """
    share_json = UnityCatalogApi(api_client).get_share(name, include_shared_data)
    click.echo(mc_pretty_format(share_json))


def shared_data_object(name):
    return {'name': name, 'data_object_type': 'TABLE'}


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Update a share.')
@click.option('--name', required=True,
              help='Name of the share to update.')
@click.option('--add-table', default=None, multiple=True,
              help='Full name of table to add to share')
@click.option('--remove-table', default=None, multiple=True,
              help='Full name of table to remove from share')
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to PATCH.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/shares'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def update_share_cli(api_client, name, add_table, remove_table, json_file, json):
    """
    Update a share.

    Calls the 'updateShare' RPC endpoint of the Unity Catalog service.
    Returns nothing.

    """
    if len(add_table) > 0 or len(remove_table) > 0:
        updates = []
        for a in add_table:
            updates.append({'action': 'ADD', 'data_object': shared_data_object(a)})
        for r in remove_table:
            updates.append({'action': 'REMOVE', 'data_object': shared_data_object(r)})
        d = {'updates': updates}
        share_json = UnityCatalogApi(api_client).update_share(name, d)
        click.echo(mc_pretty_format(share_json))
    else:
        json_cli_base(json_file, json,
                      lambda json: UnityCatalogApi(api_client).update_share(name, json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Delete a share.')
@click.option('--name', required=True,
              help='Name of the share to delete.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def delete_share_cli(api_client, name):
    """
    Delete a share.

    Calls the 'deleteShare' RPC endpoint of the Unity Catalog service.
    Returns nothing.

    """
    UnityCatalogApi(api_client).delete_share(name)


##############  Recipient Commands  ##############

@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create a new recipient.')
@click.option('--name', required=True, help='Name of new recipient.')
@click.option('--comment', default=None, required=False,
              help='Free-form text description.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def create_recipient_cli(api_client, name, comment):
    """
    Create a new recipient.

    Calls the 'createRecipient' RPC endpoint of the Unity Catalog service.
    Returns the RecipientInfo for the newly-created recipient.

    """
    recipient_json = UnityCatalogApi(api_client).create_recipient(name, comment)
    click.echo(mc_pretty_format(recipient_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='List recipients.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def list_recipients_cli(api_client):
    """
    List recipients.

    Calls the 'listRecipients' RPC endpoint of the Unity Catalog service.
    Returns array of RecipientInfos.

    """
    recipients_json = UnityCatalogApi(api_client).list_recipients()
    click.echo(mc_pretty_format(recipients_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Get a recipient.')
@click.option('--name', required=True,
              help='Name of the recipient to get.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def get_recipient_cli(api_client, name):
    """
    Get a recipient.

    Calls the 'getRecipient' RPC endpoint of the Unity Catalog service.
    Returns nothing.

    """
    recipient_json = UnityCatalogApi(api_client).get_recipient(name)
    click.echo(mc_pretty_format(recipient_json))

@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Rotate token for the recipient.')
@click.option('--name', required=True, help='Name of new recipient.')
@click.option('--existing_token_expire_in_seconds', default=None, required=False,
              help='Expire the existing token in number of seconds from now, 0 to expire it immediately.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def rotate_recipient_token_cli(api_client, name, existing_token_expire_in_seconds):
    """
    Rotate recipient token.

    Calls the 'rotateRecipientToken' RPC endpoint of the Unity Catalog service.
    Returns the RecipientInfo for the recipient with rotated tokens.

    """
    recipient_json = UnityCatalogApi(api_client).rotate_recipient_token(name, existing_token_expire_in_seconds)
    click.echo(mc_pretty_format(recipient_json))

@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Get share permissions of a recipient.')
@click.option('--name', required=True,
              help='Name of the recipient.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def get_recipient_share_permissions_cli(api_client, name):
    """
    Get a recipient's share permissions.

    Calls the 'getRecipientSharePermissions' RPC endpoint of the Unity Catalog service.
    Returns nothing.

    """
    recipient_json = UnityCatalogApi(api_client).get_recipient_share_permissions(name)
    click.echo(mc_pretty_format(recipient_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Delete a recipient.')
@click.option('--name', required=True,
              help='Name of the recipient to delete.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def delete_recipient_cli(api_client, name):
    """
    Delete a recipient.

    Calls the 'deleteRecipient' RPC endpoint of the Unity Catalog service.
    Returns nothing.

    """
    UnityCatalogApi(api_client).delete_recipient(name)


@click.group(context_settings=CONTEXT_SETTINGS,
             help='Utility to interact with Databricks unity-catalog.\n\n' +
             '**********************************************************************\n' +
             'WARNING: these commands are EXPERIMENTAL and not officially supported.\n' +
             '**********************************************************************',
             hidden=True)
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@debug_option
@profile_option
@eat_exceptions
def unity_catalog_group():  # pragma: no cover
    """
    Utility to interact with Databricks unity-catalog.
    """
    pass


# Metastore cmds:
unity_catalog_group.add_command(create_metastore_cli, name='create-metastore')
unity_catalog_group.add_command(list_metastores_cli, name='list-metastores')
unity_catalog_group.add_command(get_metastore_cli, name='get-metastore')
unity_catalog_group.add_command(update_metastore_cli, name='update-metastore')
unity_catalog_group.add_command(delete_metastore_cli, name='delete-metastore')
unity_catalog_group.add_command(metastore_summary_cli, name='metastore-summary')
unity_catalog_group.add_command(assign_metastore_cli, name='assign-metastore')
# Catalogs cmds:
unity_catalog_group.add_command(create_catalog_cli, name='create-catalog')
unity_catalog_group.add_command(list_catalogs_cli, name='list-catalogs')
unity_catalog_group.add_command(get_catalog_cli, name='get-catalog')
unity_catalog_group.add_command(update_catalog_cli, name='update-catalog')
unity_catalog_group.add_command(delete_catalog_cli, name='delete-catalog')
# Schema cmds:
unity_catalog_group.add_command(create_schema_cli, name='create-schema')
unity_catalog_group.add_command(list_schemas_cli, name='list-schemas')
unity_catalog_group.add_command(get_schema_cli, name='get-schema')
unity_catalog_group.add_command(update_schema_cli, name='update-schema')
unity_catalog_group.add_command(delete_schema_cli, name='delete-schema')
# Table cmds:
unity_catalog_group.add_command(create_table_cli, name='create-table')
unity_catalog_group.add_command(list_tables_cli, name='list-tables')
unity_catalog_group.add_command(list_tables_bulk_cli, name='list-tables-bulk')
unity_catalog_group.add_command(get_table_cli, name='get-table')
unity_catalog_group.add_command(update_table_cli, name='update-table')
unity_catalog_group.add_command(delete_table_cli, name='delete-table')
# DAC cmds:
unity_catalog_group.add_command(create_dac_cli, name='create-dac')
unity_catalog_group.add_command(list_dacs_cli, name='list-dacs')
unity_catalog_group.add_command(get_dac_cli, name='get-dac')
unity_catalog_group.add_command(delete_dac_cli, name='delete-dac')
# Credential cmds:
unity_catalog_group.add_command(create_credential_cli, name='create-storage-credential')
unity_catalog_group.add_command(list_credentials_cli, name='list-storage-credentials')
unity_catalog_group.add_command(get_credential_cli, name='get-storage-credential')
unity_catalog_group.add_command(update_credential_cli, name='update-storage-credential')
unity_catalog_group.add_command(delete_credential_cli, name='delete-storage-credential')
# External Location cmds:
unity_catalog_group.add_command(create_location_cli, name='create-external-location')
unity_catalog_group.add_command(list_locations_cli, name='list-external-locations')
unity_catalog_group.add_command(get_location_cli, name='get-external-location')
unity_catalog_group.add_command(update_location_cli, name='update-external-location')
unity_catalog_group.add_command(delete_location_cli, name='delete-external-location')
# Permissions cmds:
unity_catalog_group.add_command(get_permissions_cli, name='get-permissions')
unity_catalog_group.add_command(update_permissions_cli, name='update-permissions')
# replacePermissions endpoint not implemented on MC yet
# unity_catalog_group.add_command(replace_permissions_cli, name='replace-permissions')
unity_catalog_group.add_command(create_root_credentials_cli, name='create-root-credentials')
# Share cmds:
unity_catalog_group.add_command(create_share_cli, name='create-share')
unity_catalog_group.add_command(list_shares_cli, name='list-shares')
unity_catalog_group.add_command(get_share_cli, name='get-share')
unity_catalog_group.add_command(update_share_cli, name='update-share')
unity_catalog_group.add_command(delete_share_cli, name='delete-share')
# Recipient cmds:
unity_catalog_group.add_command(create_recipient_cli, name='create-recipient')
unity_catalog_group.add_command(list_recipients_cli, name='list-recipients')
unity_catalog_group.add_command(get_recipient_cli, name='get-recipient')
unity_catalog_group.add_command(rotate_recipient_token_cli, name='rotate-recipient-token')
unity_catalog_group.add_command(get_recipient_share_permissions_cli,
                                name='get-recipient-share-perms')
unity_catalog_group.add_command(delete_recipient_cli, name='delete-recipient')
