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

from databricks_cli.click_types import OneOfOption
from databricks_cli.configure.config import provide_api_client, profile_option, debug_option
from databricks_cli.permissions.api import PermissionsApi, PermissionTargets, PermissionLevel, \
    PermissionType, Permission, PermissionsObject, Lookups
from databricks_cli.utils import eat_exceptions, CONTEXT_SETTINGS
from databricks_cli.utils import pretty_format
from databricks_cli.version import print_version_callback, version
from databricks_cli.workspace.api import WorkspaceApi

FILTERS_HELP = 'Filters for filtering the list of users: ' + \
               'https://docs.databricks.com/api/latest/scim.html#filters'
USER_OPTIONS = ['user-id', 'user-name']
JSON_FILE_OPTIONS = ['json-file', 'json']
CREATE_USER_OPTIONS = JSON_FILE_OPTIONS + ['user-name']

PERMISSIONS_OPTION = ['group-name', 'user-name', 'service-name']

POSSIBLE_OBJECT_TYPES = 'Possible object types are: \n\t{}\n'.format(
    PermissionTargets.help_values())

POSSIBLE_PERMISSION_LEVELS = 'Possible permission levels are: \n\t{}\n'.format(
    PermissionLevel.help_values())


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Get permissions for an item.  ' + POSSIBLE_OBJECT_TYPES)
@click.option('--object_type', help='Possible object types are: {}'.format(PermissionTargets))
@click.option('--object-id', required=False)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def get_cli(api_client, object_type, object_id):
    perms_api = PermissionsApi(api_client)
    if not object_type:
        click.echo('Possible object types are: {}'.format(PermissionTargets))
    click.echo(pretty_format(perms_api.get_permissions(object_type, object_id)))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='List permission types')
@click.option('--object_type', help=POSSIBLE_OBJECT_TYPES)
@click.option('--object_id')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def list_permissions_types_cli(api_client, object_type, object_id):
    perms_api = PermissionsApi(api_client)
    if not object_type:
        click.echo('Possible object types are: {}'.format(PermissionTargets))
    click.echo(pretty_format(perms_api.get_possible_permissions(object_type, object_id)))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Add or modify permission types')
@click.option('--object_type', help=POSSIBLE_OBJECT_TYPES)
@click.option('--object_id')
@click.option('--group-name', metavar='<string>', cls=OneOfOption, default=None,
              one_of=PERMISSIONS_OPTION)
@click.option('--user-name', metavar='<string>', cls=OneOfOption, default=None,
              one_of=PERMISSIONS_OPTION)
@click.option('--service-name', metavar='<string>', cls=OneOfOption, default=None,
              one_of=PERMISSIONS_OPTION)
@click.option('--permission-level', metavar='<string>', default=None,
              required=True)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
# pylint:disable=unused-argument
def add_cli(api_client, object_type, object_id, user_name, group_name, service_name,
            permission_level):
    perms_api = PermissionsApi(api_client)

    if not user_name and not group_name and not service_name:
        click.echo('Need --user-name, --service-name or --group-name')
        return

    if not object_type:
        click.echo('Possible object types are: {}'.format(PermissionTargets))

    if not permission_level:
        click.echo('Need permission-level: {}'.format([e.value for e in PermissionLevel]))

    # Determine the type of permissions we're adding.
    if user_name:
        perm_type = PermissionType.user
        value = user_name
    elif group_name:
        perm_type = PermissionType.group
        value = group_name
    elif service_name:
        perm_type = PermissionType.service
        value = service_name
    else:
        click.echo(
            'Invalid permission-type must be one of {}'.format([e.value for e in PermissionType]))
        return

    permission = Permission(perm_type, value, Lookups.from_name(permission_level.upper()))
    all_permissions = PermissionsObject()
    all_permissions.add(permission)

    if not all_permissions.to_dict():
        click.echo('invalid permissions')
        return

    click.echo(pretty_format(perms_api.add_permissions(object_type, object_id, all_permissions)))


@click.command(context_settings=CONTEXT_SETTINGS)
@debug_option
@profile_option
@eat_exceptions
def list_permissions_targets_cli():
    click.echo(POSSIBLE_OBJECT_TYPES)


@click.command(context_settings=CONTEXT_SETTINGS)
@debug_option
@profile_option
@eat_exceptions
def list_permissions_level_cli():
    click.echo(POSSIBLE_PERMISSION_LEVELS)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Get permissions for a directory')
@click.argument('path')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def directory_cli(api_client, path):
    perms_api = PermissionsApi(api_client)
    workspace_api = WorkspaceApi(api_client)

    object_type = 'directories'
    object_ids = workspace_api.get_id_for_directory(path)

    if not object_ids:
        click.echo('Failed to find id for {}'.format(path))
        return

    # if len(object_ids) > 1:
    #     click.echo('Too many objects ({}) returned for {}'.format(len(object_ids), path))

    for object_id in object_ids:
        click.echo(pretty_format(perms_api.get_permissions(object_type, object_id)))


@click.group(context_settings=CONTEXT_SETTINGS,
             short_help='xUtility to interact with Databricks permissions api.\n' +
                        'Possible object types are: {}\n'.format(PermissionTargets))
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@debug_option
@profile_option
@eat_exceptions
def permissions_group():
    """Provide utility to interact with Databricks permissions api."""
    pass


permissions_group.add_command(list_permissions_types_cli, name='list-types')
permissions_group.add_command(get_cli, name='get')
permissions_group.add_command(add_cli, name='add')
permissions_group.add_command(list_permissions_targets_cli, name='targets')
permissions_group.add_command(list_permissions_level_cli, name='levels')

permissions_group.add_command(directory_cli, name='ls')
