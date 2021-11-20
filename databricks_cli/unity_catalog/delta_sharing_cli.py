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
@click.option('--include-shared-data', default=True,
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


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='List permissions on a share.')
@click.option('--name', required=True,
              help='Name of the share to list permissions on.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def list_share_permissions_cli(api_client, name):
    """
    List permissions on a share.

    Calls the 'getSharePermissions' RPC endpoint of the Unity Catalog service.
    Returns nothing.

    """
    perms_json = UnityCatalogApi(api_client).list_share_permissions(name)
    click.echo(mc_pretty_format(perms_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Update permissions on a share.')
@click.option('--name', required=True,
              help='Name of the share whose permissions are updated.')
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to POST.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/unity-catalog/shares/{name}/permissions'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def update_share_permissions_cli(api_client, name, json_file, json):
    """
    List permissions on a share.

    Calls the 'getSharePermissions' RPC endpoint of the Unity Catalog service.
    Returns nothing.

    """
    json_cli_base(json_file, json,
                  lambda json: UnityCatalogApi(api_client).update_share_permissions(name, json))


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
@click.option('--existing-token-expire-in-seconds', default=None, required=False,
              help='Expire the existing token in number of seconds from now,' +
                   ' 0 to expire it immediately.')
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
    recipient_json = \
        UnityCatalogApi(api_client).rotate_recipient_token(name, existing_token_expire_in_seconds)
    click.echo(mc_pretty_format(recipient_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='List share permissions of a recipient.')
@click.option('--name', required=True,
              help='Name of the recipient.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def list_recipient_permissions_cli(api_client, name):
    """
    List a recipient's share permissions.

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


##############  Provider Commands  ##############

@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create a provider.')
@click.option('--name', required=True, help='Name of the new provider.')
@click.option('--comment', default=None, required=False,
              help='Free-form text description.')
@click.option('--recipient-profile-json-file', default=None, type=click.Path(),
              help='File containing recipient profile in JSON format.')
@click.option('--recipient-profile-json', default=None, type=JsonClickType(),
              help='JSON string containing recipient profile.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def create_provider_cli(api_client, name, comment, recipient_profile_json_file,
                        recipient_profile_json):
    """
    Create a provider.

    Calls the 'createProvider' RPC endpoint of the Unity Catalog service.
    Returns the created provider info.

    """
    json_cli_base(recipient_profile_json_file, recipient_profile_json,
                  lambda json: UnityCatalogApi(api_client).create_provider(name, comment, json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='List providers.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def list_providers_cli(api_client):
    """
    List providers.

    Calls the 'listProviders' RPC endpoint of the Unity Catalog service.
    Returns array of ProviderInfos.

    """
    proviers_json = UnityCatalogApi(api_client).list_providers()
    click.echo(mc_pretty_format(proviers_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Get a provider.')
@click.option('--name', required=True,
              help='Name of the provider to get.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def get_provider_cli(api_client, name):
    """
    Get a provider.

    Calls the 'getProvider' RPC endpoint of the Unity Catalog service.
    Returns ProviderInfo.

    """
    provier_json = UnityCatalogApi(api_client).get_provider(name)
    click.echo(mc_pretty_format(provier_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Update a provider.')
@click.option('--name', required=True, help='Name of the provider to update.')
@click.option('--new_name', default=None, help='New name of the provider.')
@click.option('--comment', default=None, required=False,
              help='Free-form text description.')
@click.option('--recipient-profile-json-file', default=None, type=click.Path(),
              help='File containing recipient profile in JSON format.')
@click.option('--recipient-profile-json', default=None, type=JsonClickType(),
              help='JSON string containing recipient profile.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def update_provider_cli(api_client, name, new_name, comment, recipient_profile_json_file,
                        recipient_profile_json):
    """
    Update a provider.

    Calls the 'updateProvider' RPC endpoint of the Unity Catalog service.
    Returns the updated provider info.

    """
    if recipient_profile_json is None and recipient_profile_json_file is None:
        updated_provider = UnityCatalogApi(api_client).update_provider(name, new_name, comment)
        click.echo(mc_pretty_format(updated_provider))
    else:
        json_cli_base(recipient_profile_json_file, recipient_profile_json,
                      lambda json: UnityCatalogApi(api_client).update_provider(name, new_name,
                                                                               comment, json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='List shares of a provider.')
@click.option('--name', required=True,
              help='Name of the provider.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def list_provider_shares_cli(api_client, name):
    """
    List a provider's shares.

    Calls the 'listProviderShares' RPC endpoint of the Unity Catalog service.
    Returns array of ProviderShare.

    """
    shares_json = UnityCatalogApi(api_client).list_provider_shares(name)
    click.echo(mc_pretty_format(shares_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Delete a provider.')
@click.option('--name', required=True,
              help='Name of the provider to delete.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def delete_provider_cli(api_client, name):
    """
    Delete a provider.

    Calls the 'deleteProvider' RPC endpoint of the Unity Catalog service.
    Returns nothing.

    """
    UnityCatalogApi(api_client).delete_provider(name)


def register_delta_sharing_commands(cmd_group):
    # Share cmds:
    cmd_group.add_command(create_share_cli, name='create-share')
    cmd_group.add_command(list_shares_cli, name='list-shares')
    cmd_group.add_command(get_share_cli, name='get-share')
    cmd_group.add_command(update_share_cli, name='update-share')
    cmd_group.add_command(delete_share_cli, name='delete-share')
    cmd_group.add_command(list_share_permissions_cli, name='list-share-permissions')
    cmd_group.add_command(update_share_permissions_cli, name='update-share-permissions')

    # Recipient cmds:
    cmd_group.add_command(create_recipient_cli, name='create-recipient')
    cmd_group.add_command(list_recipients_cli, name='list-recipients')
    cmd_group.add_command(get_recipient_cli, name='get-recipient')
    cmd_group.add_command(rotate_recipient_token_cli, name='rotate-recipient-token')
    cmd_group.add_command(list_recipient_permissions_cli, name='list-recipient-permissions')
    cmd_group.add_command(delete_recipient_cli, name='delete-recipient')

    # Provider cmds:
    cmd_group.add_command(create_provider_cli, name='create-provider')
    cmd_group.add_command(list_providers_cli, name='list-providers')
    cmd_group.add_command(get_provider_cli, name='get-provider')
    cmd_group.add_command(update_provider_cli, name='update-provider')
    cmd_group.add_command(delete_provider_cli, name='delete-provider')
    cmd_group.add_command(list_provider_shares_cli, name='list-provider-shares')
