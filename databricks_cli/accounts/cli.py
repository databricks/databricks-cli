"""Provide the API methods for Databricks Accounts REST endpoint."""
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

from databricks_cli.click_types import AccountIdClickType, CredentialsIdClickType, NetworkIdClickType, \
    WorkspaceIdClickType, JsonClickType
from databricks_cli.accounts.api import AccountsApi
from databricks_cli.utils import eat_exceptions, CONTEXT_SETTINGS, pretty_format, json_cli_base
from databricks_cli.configure.config import provide_api_client, profile_option, debug_option
from databricks_cli.version import print_version_callback, version


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Create a new credentials object with AWS IAM role reference.")
@click.option('--account-id', required=True, type=AccountIdClickType(),
              help=AccountIdClickType.help)
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to POST to /api/2.0/accounts/{account_id}/credentials.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/accounts/{account_id}/credentials'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def create_credentials_cli(api_client, account_id, json_file, json):
    """Create a new credentials object with AWS IAM role reference."""
    json_cli_base(json_file, json, lambda json: AccountsApi(api_client).create_credentials(account_id, json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Get the credentials object with the given credentials id.")
@click.option('--account-id', required=True, type=AccountIdClickType(),
              help=AccountIdClickType.help)
@click.option("--credentials-id", required=True)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def get_credentials_cli(api_client, account_id, credentials_id):
    """Get the credentials object with the given credentials id."""
    content = AccountsApi(api_client).get_credentials(account_id, credentials_id)
    click.echo(pretty_format(content))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Delete the credentials object for the given credentials id.")
@click.option('--account-id', required=True, type=AccountIdClickType(),
              help=AccountIdClickType.help)
@click.option("--credentials-id", required=True)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def delete_credentials_cli(api_client, account_id, credentials_id):
    """Delete the credentials object for the given credentials id."""
    content = AccountsApi(api_client).delete_credentials(account_id, credentials_id)
    click.echo(pretty_format(content))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Create a new storage config object with AWS bucket reference.")
@click.option('--account-id', required=True, type=AccountIdClickType(),
              help=AccountIdClickType.help)
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to POST to /api/2.0/accounts/{account_id}/storage-configurations.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/accounts/{account_id}/storage-configurations'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def create_storage_config_cli(api_client, account_id, json_file, json):
    """Create a new storage config object with AWS bucket reference."""
    json_cli_base(json_file, json, lambda json: AccountsApi(api_client).create_storage_config(account_id, json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Get the storage config object with the given storage config id.")
@click.option('--account-id', required=True, type=AccountIdClickType(),
              help=AccountIdClickType.help)
@click.option("--storage-config-id", required=True)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def get_storage_config_cli(api_client, account_id, storage_config_id):
    """Get the storage config object with the given storage config id."""
    content = AccountsApi(api_client).get_storage_config(account_id, storage_config_id)
    click.echo(pretty_format(content))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Delete the storage config object for the given storage config id.")
@click.option('--account-id', required=True, type=AccountIdClickType(),
              help=AccountIdClickType.help)
@click.option("--storage-config-id", required=True)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def delete_storage_config_cli(api_client, account_id, storage_config_id):
    """Delete the storage config object for the given storage config id."""
    content = AccountsApi(api_client).delete_storage_config(account_id, storage_config_id)
    click.echo(pretty_format(content))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Create a new network object with AWS network infrastructure reference.")
@click.option('--account-id', required=True, type=AccountIdClickType(),
              help=AccountIdClickType.help)
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to POST to /api/2.0/accounts/{account_id}/networks.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/accounts/{account_id}/networks'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def create_network_cli(api_client, account_id, json_file, json):
    """Create a new network object with AWS network infrastructure reference."""
    json_cli_base(json_file, json, lambda json: AccountsApi(api_client).create_network(account_id, json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Get the network object with the given network id.")
@click.option('--account-id', required=True, type=AccountIdClickType(),
              help=AccountIdClickType.help)
@click.option("--network-id", required=True)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def get_network_cli(api_client, account_id, network_id):
    """Get the network object with the given network id."""
    content = AccountsApi(api_client).get_network(account_id, network_id)
    click.echo(pretty_format(content))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Delete the network object for the given network id.")
@click.option('--account-id', required=True, type=AccountIdClickType(),
              help=AccountIdClickType.help)
@click.option("--network-id", required=True)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def delete_network_cli(api_client, account_id, network_id):
    """Delete the network object for the given network id."""
    content = AccountsApi(api_client).delete_network(account_id, network_id)
    click.echo(pretty_format(content))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Create a new workspace with required references.")
@click.option('--account-id', required=True, type=AccountIdClickType(),
              help=AccountIdClickType.help)
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to POST to /api/2.0/accounts/{account_id}/workspaces.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/accounts/{account_id}/workspaces'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def create_workspace_cli(api_client, account_id, json_file, json):
    """Create a new workspace with required references."""
    json_cli_base(json_file, json, lambda json: AccountsApi(api_client).create_workspace(account_id, json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Get the workspace details with the given workspace id.")
@click.option('--account-id', required=True, type=AccountIdClickType(),
              help=AccountIdClickType.help)
@click.option("--workspace-id", required=True)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def get_workspace_cli(api_client, account_id, workspace_id):
    """Get the workspace details with the given workspace id."""
    content = AccountsApi(api_client).get_workspace(account_id, workspace_id)
    click.echo(pretty_format(content))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Delete the workspace for the given workspace id.")
@click.option('--account-id', required=True, type=AccountIdClickType(),
              help=AccountIdClickType.help)
@click.option("--workspace-id", required=True)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def delete_workspace_cli(api_client, account_id, workspace_id):
    """Delete the workspace for the given workspace id."""
    content = AccountsApi(api_client).delete_workspace(account_id, workspace_id)
    click.echo(pretty_format(content))


@click.group(context_settings=CONTEXT_SETTINGS,
             short_help='Utility to interact with Databricks accounts.')
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@debug_option
@profile_option
@eat_exceptions
def accounts_group():
    """
    Utility to interact with Databricks accounts.
    """
    pass

accounts_group.add_command(create_credentials_cli, name="create-credentials")
accounts_group.add_command(get_credentials_cli, name="get-credentials")
accounts_group.add_command(delete_credentials_cli, name="delete-credentials")
accounts_group.add_command(create_storage_config_cli, name="create-storage-config")
accounts_group.add_command(get_storage_config_cli, name="get-storage-config")
accounts_group.add_command(delete_storage_config_cli, name="delete-storage-config")
accounts_group.add_command(create_network_cli, name="create-network")
accounts_group.add_command(get_network_cli, name="get-network")
accounts_group.add_command(delete_network_cli, name="delete-network")
accounts_group.add_command(create_workspace_cli, name="create-workspace")
accounts_group.add_command(get_workspace_cli, name="get-workspace")
accounts_group.add_command(delete_workspace_cli, name="delete-workspace")
