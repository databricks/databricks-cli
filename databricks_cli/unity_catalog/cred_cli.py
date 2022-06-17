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

from databricks_cli.click_types import JsonClickType
from databricks_cli.configure.config import provide_api_client, profile_option, debug_option
from databricks_cli.unity_catalog.api import UnityCatalogApi
from databricks_cli.unity_catalog.utils import hide, json_file_help, json_string_help, \
    mc_pretty_format
from databricks_cli.utils import eat_exceptions, CONTEXT_SETTINGS, json_cli_base


#############  Storage Credential Commands  ############


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create storage credential.')
@click.option('--skip-validation', '-s', 'skip_val', is_flag=True, default=False,
              help='Skip the validation of new credential info before creation')
@click.option('--json-file', default=None, type=click.Path(),
              help=json_file_help(method='POST', path='/storage-credentials'))
@click.option('--json', default=None, type=JsonClickType(),
              help=json_string_help(method='POST', path='/storage-credentials'))
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

    The public specification for the JSON request is in development.
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
              help=json_file_help(method='PATCH', path='/storage-credentials/{name}'))
@click.option('--json', default=None, type=JsonClickType(),
              help=json_string_help(method='PATCH', path='/storage-credentials/{name}'))
@debug_option
@profile_option
# See comment for create-storage-credential
#@eat_exceptions
@provide_api_client
def update_credential_cli(api_client, name, skip_val, json_file, json):
    """
    Update a storage credential.

    The public specification for the JSON request is in development.
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
    """
    UnityCatalogApi(api_client).delete_storage_credential(name, force)


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

    # Register command group.
    storage_credentials_group.add_command(create_credential_cli, name='create')
    storage_credentials_group.add_command(list_credentials_cli, name='list')
    storage_credentials_group.add_command(get_credential_cli, name='get')
    storage_credentials_group.add_command(update_credential_cli, name='update')
    storage_credentials_group.add_command(delete_credential_cli, name='delete')
    cmd_group.add_command(storage_credentials_group, name='storage-credentials')
