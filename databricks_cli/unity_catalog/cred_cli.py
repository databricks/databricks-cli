# Databricks CLI
# Copyright 2022 Databricks, Inc.
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

from databricks_cli.click_types import DacIdClickType, JsonClickType, MetastoreIdClickType
from databricks_cli.configure.config import provide_api_client, profile_option, debug_option
from databricks_cli.unity_catalog.api import UnityCatalogApi
from databricks_cli.unity_catalog.utils import mc_pretty_format, hide
from databricks_cli.utils import eat_exceptions, CONTEXT_SETTINGS, json_cli_base


#############  Storage Credential Commands  ############


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create storage credential.')
@click.option('--skip-validation', '-s', 'skip_val', is_flag=True, default=False,
              help='Skip the validation of new credential info before creation')
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to POST.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/unity-catalog/storage-credentials'))
@debug_option
@profile_option
# UC's createStorageCredential returns a 401 when validation fails; that translates to
# a misleading error when eat_exceptions is enabled:
#   Your authentication information may be incorrect. Please reconfigure with ``dbfs configure``
# Until that is fixed (should return a 400), show full error trace.
#@eat_exceptions
@provide_api_client
def create_credential_cli(api_client, skip_val, json_file, json):
    """
    Create new storage credential.

    Calls the 'createStorageCredential' RPC endpoint of the Unity Catalog service.
    The public specification for the JSON request is in development.
    Returns the properties of the newly-created Storage Credential.

    """
    json_cli_base(json_file, json,
                  lambda json: UnityCatalogApi(api_client).create_storage_credential(json,
                                                                                     skip_val),
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
@click.option('--skip-validation', '-s', 'skip_val', is_flag=True, default=False,
              help='Skip the validation of new credential info before update')
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to PATCH.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/storage-credentials'))
@debug_option
@profile_option
# See comment for create-storage-credential
#@eat_exceptions
@provide_api_client
def update_credential_cli(api_client, name, skip_val, json_file, json):
    """
    Update a storage credential.

    Calls the 'updateStorageCredential' RPC endpoint of the Unity Catalog service.
    The public specification for the JSON request is in development.
    Returns nothing.

    """
    json_cli_base(json_file, json,
                  lambda json: UnityCatalogApi(api_client).update_storage_credential(name,
                                                                                     json,
                                                                                     skip_val),
                  encode_utf8=True)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Delete a storage credential.')
@click.option('--name', required=True,
              help='Name of the storage credential to delete.')
@click.option('--force', '-f', is_flag=True, default=False,
              help='Force deletion even if credential has dependent tables/locations')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def delete_credential_cli(api_client, name, force):
    """
    Delete a storage credential.

    Calls the 'deleteStorageCredential' RPC endpoint of the Unity Catalog service.
    Returns nothing.

    """
    UnityCatalogApi(api_client).delete_storage_credential(name, force)


#############  Data Access Configuration Commands  ############


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create data access configuration.')
@click.option('--metastore-id', required=True, type=MetastoreIdClickType(),
              help='Unique identifier of the metastore parent of the DAC.')
@click.option('--skip-validation', '-s', 'skip_val', is_flag=True, default=False,
              help='Skip the validation of new DAC info before creation')
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to POST.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/unity-catalog/data-access-configurations'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def create_dac_cli(api_client, metastore_id, skip_val, json_file, json):
    """
    Create new data access configuration.

    Calls the 'createDataAccessConfiguration' RPC endpoint of the Unity Catalog service.
    The public specification for the JSON request is in development.
    Returns the properties of the newly-created DAC.

    """
    json_cli_base(json_file, json,
                  lambda json: UnityCatalogApi(api_client).create_dac(metastore_id, json,
                                                                      skip_val),
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


@click.group()
def storage_credentials_group():  # pragma: no cover
    pass


def register_cred_commands(cmd_group):
    # Register deprecated "verb-noun" commands for backward compatibility.
    cmd_group.add_command(hide(create_credential_cli), name='create-storage-credential')
    cmd_group.add_command(hide(list_credentials_cli), name='list-storage-credentials')
    cmd_group.add_command(hide(get_credential_cli), name='get-storage-credential')
    cmd_group.add_command(hide(update_credential_cli), name='update-storage-credential')
    cmd_group.add_command(hide(delete_credential_cli), name='delete-storage-credential')

    # DAC cmds: [TO BE DEPRECATED ONCE STORAGE CREDENTIALS ARE FULLY SUPPORTED]
    cmd_group.add_command(hide(create_dac_cli), name='create-dac')
    cmd_group.add_command(hide(list_dacs_cli), name='list-dacs')
    cmd_group.add_command(hide(get_dac_cli), name='get-dac')
    cmd_group.add_command(hide(delete_dac_cli), name='delete-dac')

    # Register command group.
    storage_credentials_group.add_command(create_credential_cli, name='create')
    storage_credentials_group.add_command(list_credentials_cli, name='list')
    storage_credentials_group.add_command(get_credential_cli, name='get')
    storage_credentials_group.add_command(update_credential_cli, name='update')
    storage_credentials_group.add_command(delete_credential_cli, name='delete')
    cmd_group.add_command(storage_credentials_group, name='storage-credentials')
