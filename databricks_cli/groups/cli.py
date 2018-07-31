"""Provide the API methods for GROUPs Databricks REST endpoint."""
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

from databricks_cli.groups.api import GroupsApi
from databricks_cli.utils import eat_exceptions, CONTEXT_SETTINGS, pretty_format
from databricks_cli.configure.config import provide_api_client, profile_option
from databricks_cli.version import print_version_callback, version


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Add an existing group to another existing group.")
@click.option("--parent-name", required=True,
              help=("Name of the parent group to which the new member will be "
                    " added. This field is required."))
@click.option("--user-name", required=False,
              help="If user_name, the user name.")
@click.option("--group-name", required=False,
              help="If group_name, the group name.")
@profile_option
@eat_exceptions
@provide_api_client
def add_member_cli(api_client, parent_name, user_name, group_name):
    """Add a user or group to a group."""
    member_type = None
    member_name = None
    if user_name:
        member_type = "user"
        member_name = user_name
    elif group_name:
        member_type = "group"
        member_name = group_name
    GroupsApi(api_client).add_member(parent_name, member_type, member_name)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Create a new group with the given name.")
@click.option("--group-name", required=True)
@profile_option
@eat_exceptions
@provide_api_client
def create_cli(api_client, group_name):
    """Create a new group with the given name."""
    content = GroupsApi(api_client).create(group_name)
    click.echo(pretty_format(content))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Return all of the members of a particular group.")
@click.option("--group-name", required=True)
@profile_option
@eat_exceptions
@provide_api_client
def list_members_cli(api_client, group_name):
    """Return all of the members of a particular group."""
    content = GroupsApi(api_client).list_members(group_name)
    click.echo(pretty_format(content))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Return all of the groups in an organization.")
@profile_option
@eat_exceptions
@provide_api_client
def list_all_cli(api_client):
    """Return all of the groups in an organization."""
    content = GroupsApi(api_client).list_all()
    click.echo(pretty_format(content))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Retrieve all groups in which a given user or group is a member.")
@click.option("--user-name", required=False)
@click.option("--group-name", required=False)
@profile_option
@eat_exceptions
@provide_api_client
def list_parents_cli(api_client, user_name=None, group_name=None):
    """Retrieve all groups in which a given user or group is a member."""
    member_type = None
    member_name = None
    if user_name:
        member_type = "user"
        member_name = user_name
    elif group_name:
        member_type = "group"
        member_name = group_name
    content = GroupsApi(api_client).list_parents(member_type, member_name)
    click.echo(pretty_format(content))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Removes a user or group from a group.")
@click.option("--parent-name", required=True)
@click.option("--user-name", required=False)
@click.option("--group-name", required=False)
@profile_option
@eat_exceptions
@provide_api_client
def remove_member_cli(api_client, parent_name, user_name=None,
                      group_name=None):
    """Remove a user or group from a group."""
    member_type = None
    member_name = None
    if user_name:
        member_type = "user"
        member_name = user_name
    elif group_name:
        member_type = "group"
        member_name = group_name
    GroupsApi(api_client).remove_member(parent_name=parent_name,
                                        member_type=member_type,
                                        member_name=member_name)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Remove a group from this organization.")
@click.option("--group-name", required=False)
@profile_option
@eat_exceptions
@provide_api_client
def delete_cli(api_client, group_name):
    """Remove a group from this organization."""
    content = GroupsApi(api_client).delete(group_name)
    click.echo(pretty_format(content))


@click.group(context_settings=CONTEXT_SETTINGS,
             short_help="Utility to interact with Databricks groups.")
@click.option("--version", "-v", is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@profile_option
@eat_exceptions
def groups_group():
    """Provide utility to interact with Databricks groups."""
    pass


groups_group.add_command(add_member_cli, name="add-member")
groups_group.add_command(create_cli, name="create")
groups_group.add_command(list_members_cli, name="list-members")
groups_group.add_command(list_all_cli, name="list")
groups_group.add_command(list_parents_cli, name="list-parents")
groups_group.add_command(remove_member_cli, name="remove-member")
groups_group.add_command(delete_cli, name="delete")
