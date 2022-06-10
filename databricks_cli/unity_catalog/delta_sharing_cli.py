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
from databricks_cli.unity_catalog.utils import mc_pretty_format, hide
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

    Prints the newly-created share.
    Returns nothing.

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

    Prints shares.
    Returns nothing.

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

    Prints the corresponding share.
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

    Prints share permissions on a share.
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
    Update permissions on a share.

    Prints the updated share permissions.
    The public specification for the JSON request is in development.
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

    Prints the updated share.
    The public specification for the JSON request is in development.
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

    Prints nothing.
    Returns nothing.

    """
    UnityCatalogApi(api_client).delete_share(name)


##############  Recipient Commands  ##############

@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create a new recipient.')
@click.option('--name', required=True, help='Name of new recipient.')
@click.option('--comment', default=None, required=False,
              help='Free-form text description.')
@click.option('--sharing-code', default=None, required=False,
              help='A one-time sharing code shared by the data recipient offline.')
@click.option('--allowed_ip_address', default=None, required=False, multiple=True,
              help=(
                  'IP address in CIDR notation that is allowed to use delta sharing. '
                  'Supports multiple options.'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def create_recipient_cli(api_client, name, comment, sharing_code, allowed_ip_address):
    """
    Create a new recipient.

    Prints the newly-created recipient.
    Returns nothing.

    """
    recipient_json = UnityCatalogApi(api_client).create_recipient(
        name, comment, sharing_code, allowed_ip_address)
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

    Prints recipients.
    Returns nothing.

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

    Prints the corresponding recipient.
    Returns nothing.

    """
    recipient_json = UnityCatalogApi(api_client).get_recipient(name)
    click.echo(mc_pretty_format(recipient_json))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Update a recipient.')
@click.option('--name', required=True,
              help='Name of the recipient who needs to be updated.')
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to PATCH.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/unity-catalog/recipients/{name}'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def update_recipient_cli(api_client, name, json_file, json):
    """
    Update a recipient.

    Prints the updated recipient.
    The public specification for the JSON request is in development.
    Returns nothing.

    """
    json_cli_base(json_file, json,
                  lambda json: UnityCatalogApi(api_client).update_recipient(name, json))


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

    Prints the recipient with rotated tokens.
    Returns nothing.

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

    Prints share permissions of a recipient.
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

    Prints nothing.
    Returns nothing.

    """
    UnityCatalogApi(api_client).delete_recipient(name)


##############  Provider Commands  ##############

@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create a provider.')
@click.option('--name', required=True, help='Name of the new provider.')
@click.option('--comment', default=None, required=False,
              help='Free-form text description.')
@click.option('--recipient-profile-json-file', default=None, required=False, type=click.Path(),
              help='File containing recipient profile in JSON format.')
@click.option('--recipient-profile-json', default=None, required=False, type=JsonClickType(),
              help='JSON string containing recipient profile.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def create_provider_cli(api_client, name, comment, recipient_profile_json_file,
                        recipient_profile_json):
    """
    Create a provider.

    Prints the newly-created provider.
    The public specification for the JSON request is in development.
    Returns nothing.

    """
    if recipient_profile_json is None and recipient_profile_json_file is None:
        created_provider = UnityCatalogApi(api_client).create_provider(
            name, comment, recipient_profile=None)
        click.echo(mc_pretty_format(created_provider))
    json_cli_base(recipient_profile_json_file, recipient_profile_json,
                  lambda json: UnityCatalogApi(api_client).create_provider(name, comment, json),
                  error_msg='Either --recipient-profile-json-file or ' +
                  '--recipient-profile-json should be provided')


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='List providers.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def list_providers_cli(api_client):
    """
    List providers.

    Prints providers.
    Returns nothing.

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

    Prints the corresponding provider.
    Returns nothing.

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

    Prints the updated provider info.
    The public specification for the JSON request is in development.
    Returns nothing.

    """
    if recipient_profile_json is None and recipient_profile_json_file is None:
        updated_provider = UnityCatalogApi(api_client).update_provider(name, new_name, comment)
        click.echo(mc_pretty_format(updated_provider))
    else:
        json_cli_base(recipient_profile_json_file, recipient_profile_json,
                      lambda json: UnityCatalogApi(api_client).update_provider(name, new_name,
                                                                               comment, json),
                      error_msg='Either --recipient-profile-json-file or ' +
                      '--recipient-profile-json should be provided')


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

    Prints shares of a provider.
    Returns nothing.

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

    Prints nothing.
    Returns nothing.

    """
    UnityCatalogApi(api_client).delete_provider(name)


@click.group()
def shares_group():  # pragma: no cover
    pass


def register_shares_commands(cmd_group):
    # Register deprecated "verb-noun" commands for backward compatibility.
    cmd_group.add_command(hide(create_share_cli), name='create-share')
    cmd_group.add_command(hide(list_shares_cli), name='list-shares')
    cmd_group.add_command(hide(get_share_cli), name='get-share')
    cmd_group.add_command(hide(update_share_cli), name='update-share')
    cmd_group.add_command(hide(delete_share_cli), name='delete-share')
    cmd_group.add_command(hide(list_share_permissions_cli), name='list-share-permissions')
    cmd_group.add_command(hide(update_share_permissions_cli), name='update-share-permissions')

    # Register command group.
    shares_group.add_command(create_share_cli, name='create')
    shares_group.add_command(list_shares_cli, name='list')
    shares_group.add_command(get_share_cli, name='get')
    shares_group.add_command(update_share_cli, name='update')
    shares_group.add_command(delete_share_cli, name='delete')
    shares_group.add_command(list_share_permissions_cli, name='list-permissions')
    shares_group.add_command(update_share_permissions_cli, name='update-permissions')
    cmd_group.add_command(shares_group, name='shares')


@click.group()
def recipients_group():  # pragma: no cover
    pass


def register_recipients_commands(cmd_group):
    # Register deprecated "verb-noun" commands for backward compatibility.
    cmd_group.add_command(hide(create_recipient_cli), name='create-recipient')
    cmd_group.add_command(hide(list_recipients_cli), name='list-recipients')
    cmd_group.add_command(hide(get_recipient_cli), name='get-recipient')
    cmd_group.add_command(hide(update_recipient_cli), name='update-recipient')
    cmd_group.add_command(hide(rotate_recipient_token_cli), name='rotate-recipient-token')
    cmd_group.add_command(hide(list_recipient_permissions_cli), name='list-recipient-permissions')
    cmd_group.add_command(hide(delete_recipient_cli), name='delete-recipient')

    # Register command group.
    recipients_group.add_command(create_recipient_cli, name='create')
    recipients_group.add_command(list_recipients_cli, name='list')
    recipients_group.add_command(get_recipient_cli, name='get')
    recipients_group.add_command(update_recipient_cli, name='update')
    recipients_group.add_command(rotate_recipient_token_cli, name='rotate-token')
    recipients_group.add_command(list_recipient_permissions_cli, name='list-permissions')
    recipients_group.add_command(delete_recipient_cli, name='delete')
    cmd_group.add_command(recipients_group, name='recipients')


@click.group()
def providers_group():  # pragma: no cover
    pass


def register_providers_commands(cmd_group):
    # Register deprecated "verb-noun" commands for backward compatibility.
    cmd_group.add_command(hide(create_provider_cli), name='create-provider')
    cmd_group.add_command(hide(list_providers_cli), name='list-providers')
    cmd_group.add_command(hide(get_provider_cli), name='get-provider')
    cmd_group.add_command(hide(update_provider_cli), name='update-provider')
    cmd_group.add_command(hide(delete_provider_cli), name='delete-provider')
    cmd_group.add_command(hide(list_provider_shares_cli), name='list-provider-shares')

    # Register command group.
    providers_group.add_command(create_provider_cli, name='create')
    providers_group.add_command(list_providers_cli, name='list')
    providers_group.add_command(get_provider_cli, name='get')
    providers_group.add_command(update_provider_cli, name='update')
    providers_group.add_command(delete_provider_cli, name='delete')
    providers_group.add_command(list_provider_shares_cli, name='list-shares')
    cmd_group.add_command(providers_group, name='providers')


def register_delta_sharing_commands(cmd_group):
    register_shares_commands(cmd_group)
    register_recipients_commands(cmd_group)
    register_providers_commands(cmd_group)
