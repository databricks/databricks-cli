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

from json import loads as json_loads

import click

from databricks_cli.click_types import OneOfOption, OutputClickType, JsonClickType, \
    OptionalOneOfOption
from databricks_cli.configure.config import provide_api_client, profile_option, debug_option
from databricks_cli.scim.api import ScimApi
from databricks_cli.utils import eat_exceptions, CONTEXT_SETTINGS, pretty_format
from databricks_cli.version import print_version_callback, version

FILTERS_HELP = 'Filters for filtering the list of users: ' + \
               'https://docs.databricks.com/api/latest/scim.html#filters'
USER_OPTIONS = ['user-id', 'user-name']
JSON_FILE_OPTIONS = ['json-file', 'json']
CREATE_USER_OPTIONS = JSON_FILE_OPTIONS + ['user-name']

GROUP_OPTIONS = ['group-id', 'group-name']


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='List users using the scim api')
@click.option('--filters', default=None, required=False, help=FILTERS_HELP)
@click.option('--output', default=None, help=OutputClickType.help, type=OutputClickType())
# having an active flag would be really nice.
@click.option('--active/--inactive', default=None, required=False,
              help="display only active or inactive users")
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
# pylint:disable=unused-argument
def list_users_cli(api_client, filters, active, output):
    content = ScimApi(api_client).list_users(filters=filters, active=active)
    click.echo(pretty_format(content))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Get user information by id or name')
@click.option('--user-id', metavar='<int>', cls=OneOfOption, default=None, one_of=USER_OPTIONS)
@click.option('--user-name', metavar='<string>', cls=OneOfOption, default=None, one_of=USER_OPTIONS)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def get_user_cli(api_client, user_id, user_name):
    content = ScimApi(api_client).get_user(user_id=user_id, user_name=user_name)
    click.echo(pretty_format(content))


def validate_user_params(json_file, json, user_name, groups, entitlements, roles):
    if not bool(user_name) and (bool(groups) or bool(entitlements) or bool(roles)):
        raise RuntimeError('Either --user-name or --json-file or --json should be provided')

    if not bool(user_name) and not bool(json_file) ^ bool(json):
        raise RuntimeError('Either --user-name or --json-file or --json should be provided')

    if bool(user_name) and (bool(json_file) or bool(json)):
        raise RuntimeError('Either --user-name or --json-file or --json should be provided')


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create a user')
@click.option('--json-file', cls=OneOfOption, default=None, type=click.Path(),
              one_of=CREATE_USER_OPTIONS,
              help='File containing JSON request to POST to /api/2.0/preview/scim/v2/Users.')
@click.option('--json', cls=OneOfOption, default=None, type=JsonClickType(),
              one_of=CREATE_USER_OPTIONS,
              help=JsonClickType.help('/api/2.0/preview/scim/v2/Users'))
@click.option('--user-name', metavar='<string>', cls=OneOfOption, default=None,
              one_of=CREATE_USER_OPTIONS)
@click.option('--groups', metavar='<string>', is_flag=False, default=None, type=click.STRING,
              help='Groups the user should be a member of', multiple=True, required=False)
@click.option('--entitlements', metavar='<string>', is_flag=False, default=None, type=click.STRING,
              help='Entitlements the user should have', multiple=True, required=False)
@click.option('--roles', metavar='<string>', is_flag=False, default=None, type=click.STRING,
              help='Roles the user should have', multiple=True, required=False)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def create_user_cli(api_client, json_file, json, user_name, groups, entitlements, roles):
    validate_user_params(json_file, json, user_name, groups, entitlements, roles)

    client = ScimApi(api_client)
    if user_name:
        content = client.create_user(user_name=user_name, groups=groups, entitlements=entitlements,
                                     roles=roles)
    else:
        if json_file:
            with open(json_file, 'r') as f:
                json = f.read()
        deser_json = json_loads(json)
        content = client.create_user_json(json=deser_json)

    click.echo(pretty_format(content))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='NOT IMPLEMENTED')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
# pylint:disable=unused-argument
def update_user_by_id_cli(api_client, user_id, operation, path, values):
    content = ScimApi(api_client)
    click.echo(pretty_format(content))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='NOT IMPLEMENTED')
@click.option("--force", required=False, default=False)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
# pylint:disable=unused-argument
def overwrite_user_by_id_cli(api_client, user_id, user_name, groups, entitlements, roles):
    content = ScimApi(api_client)
    click.echo(pretty_format(content))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Delete a user.  User is retained for 30 days then purged.')
@click.option('--user-id', metavar='<int>', cls=OneOfOption, default=None, one_of=USER_OPTIONS)
@click.option('--user-name', metavar='<string>', cls=OneOfOption, default=None, one_of=USER_OPTIONS)
@click.option("--force", required=False, default=False)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def delete_user_cli(api_client, user_id, user_name, force):
    if not force and not click.confirm(
            'Do you want to remove "{}"'.format(user_name)):
        click.echo('Not removing "{}"'.format(user_name))
        return

    click.echo('REMOVING "{}"'.format(user_name))
    content = ScimApi(api_client).delete_user(user_id=user_id, user_name=user_name)
    click.echo(pretty_format(content))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='List groups')
@click.option('--filters', default=None, required=False, help=FILTERS_HELP)
@click.option('--group-id', metavar='<int>', cls=OptionalOneOfOption, default=None,
              one_of=GROUP_OPTIONS)
@click.option('--group-name', metavar='<string>', cls=OptionalOneOfOption, default=None,
              one_of=GROUP_OPTIONS)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def groups_list_cli(api_client, filters, group_id, group_name):
    content = ScimApi(api_client).list_groups(filters=filters, group_id=group_id,
                                              group_name=group_name)
    click.echo(pretty_format(content))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Get group information by id or name')
@click.option('--group-id', metavar='<int>', cls=OneOfOption, default=None, one_of=GROUP_OPTIONS)
@click.option('--group-name', metavar='<string>', cls=OneOfOption, default=None,
              one_of=GROUP_OPTIONS)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def groups_get_cli(api_client, group_id, group_name):
    content = ScimApi(api_client).get_group(group_id=group_id, group_name=group_name)
    click.echo(pretty_format(content))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Create a group')
@click.option('--group-name', required=True)
@click.option('--member', 'members', is_flag=False, default=None, metavar='<members>',
              type=click.STRING,
              help='members to add to the group', multiple=True, required=False)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def groups_create_cli(api_client, group_name, members):
    content = ScimApi(api_client).create_group(group_name, members)
    click.echo(pretty_format(content))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='NOT IMPLEMENTED')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
# pylint:disable=unused-argument
def groups_update_cli(api_client, group_id, operation, path, values):
    content = ScimApi(api_client)
    click.echo(pretty_format(content))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Delete a group.')
@click.option('--group-id', metavar='<int>', cls=OneOfOption, default=None, one_of=GROUP_OPTIONS)
@click.option('--group-name', metavar='<string>', cls=OneOfOption, default=None,
              one_of=GROUP_OPTIONS)
@click.option("--force", required=False, default=False)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def groups_delete_cli(api_client, group_id, group_name):
    content = ScimApi(api_client).delete_group(group_id=group_id, group_name=group_name)
    click.echo(pretty_format(content))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Add a user to a group')
@click.option('--group-id', metavar='<int>', cls=OneOfOption, default=None, one_of=GROUP_OPTIONS)
@click.option('--group-name', metavar='<string>', cls=OneOfOption, default=None,
              one_of=GROUP_OPTIONS)
@click.option('--user-id', metavar='<int>', cls=OneOfOption, default=None, one_of=USER_OPTIONS)
@click.option('--user-name', metavar='<string>', cls=OneOfOption, default=None, one_of=USER_OPTIONS)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def groups_add_user_cli(api_client, group_id, group_name, user_id, user_name):
    content = ScimApi(api_client).add_user_to_group(group_id=group_id, group_name=group_name,
                                                    user_id=user_id,
                                                    user_name=user_name)
    click.echo(pretty_format(content))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Remove a user from a group.')
@click.option('--group-id', metavar='<int>', cls=OneOfOption, default=None, one_of=GROUP_OPTIONS)
@click.option('--group-name', metavar='<string>', cls=OneOfOption, default=None,
              one_of=GROUP_OPTIONS)
@click.option('--user-id', metavar='<int>', cls=OneOfOption, default=None, one_of=USER_OPTIONS)
@click.option('--user-name', metavar='<string>', cls=OneOfOption, default=None, one_of=USER_OPTIONS)
@click.option("--force", required=False, default=False)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def groups_remove_user_cli(api_client, group_id, group_name, user_id, user_name, force):
    if not force and not click.confirm(
            'Do you want to remove "{}" from group "{}"'.format(user_name, group_name)):
        click.echo('Not removing "{}" from group "{}"'.format(user_name, group_name))
        return

    click.echo('REMOVING "{}" from group "{}"'.format(user_name, group_name))
    content = ScimApi(api_client).remove_user_from_group(group_id=group_id, group_name=group_name,
                                                         user_id=user_id,
                                                         user_name=user_name)
    click.echo(pretty_format(content))


@click.group(context_settings=CONTEXT_SETTINGS,
             short_help='Utility to interact with Databricks scim api.\n')
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@debug_option
@profile_option
@eat_exceptions
def scim_group():
    """Provide utility to interact with Databricks scim api."""
    pass


@click.group(context_settings=CONTEXT_SETTINGS,
             short_help='Utility to interact with Databricks scim api.\n')
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@debug_option
@profile_option
@eat_exceptions
def scim_groups_group():
    """Provide utility to interact with Databricks scim groups api."""
    pass


scim_group.add_command(list_users_cli, name='list-users')
scim_group.add_command(get_user_cli, name='get-user')
scim_group.add_command(create_user_cli, name='create-user')
scim_group.add_command(update_user_by_id_cli, name='update-user')
scim_group.add_command(overwrite_user_by_id_cli, name='overwrite-user')
scim_group.add_command(delete_user_cli, name='delete-user')
scim_group.add_command(scim_groups_group, name='groups')

# add the aliases
scim_group.add_command(groups_list_cli, name='list-groups')
scim_group.add_command(groups_get_cli, name='get-group')
scim_group.add_command(groups_create_cli, name='create-group')
scim_group.add_command(groups_update_cli, name='update-group')
scim_group.add_command(groups_delete_cli, name='delete-group')

scim_groups_group.add_command(groups_list_cli, name='list')
scim_groups_group.add_command(groups_get_cli, name='get')
scim_groups_group.add_command(groups_create_cli, name='create')
scim_groups_group.add_command(groups_add_user_cli, name='add-user')
scim_groups_group.add_command(groups_remove_user_cli, name='remove-user')
scim_groups_group.add_command(groups_delete_cli, name='delete')
# scim_groups_group.add_command(group_add_user_cli, name='delete')
