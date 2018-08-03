"""Implement Databricks Groups API, interfacing with the GroupsService."""
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
from databricks_cli.sdk import GroupsService
from databricks_cli.groups.exceptions import GroupsException


class GroupsApi(object):
    """Implement the databricks '2.0/groups' API Interface."""

    def __init__(self, api_client):
        self.client = GroupsService(api_client)

    def add_member(self, parent_name, member_type, member_name):
        """Add a user or group to a group.

        member_type is either 'group' or 'user'.
        member_name is the name of the member.
        """
        if member_type == "group":
            return self.client.add_to_group(parent_name=parent_name,
                                            group_name=member_name)
        elif member_type == "user":
            return self.client.add_to_group(parent_name=parent_name,
                                            user_name=member_name)
        else:
            raise GroupsException("Invalid 'member_type' {}".format(
                member_type
            ))

    def create(self, group_name):
        """Create a new group with the given name."""
        return self.client.create_group(group_name)

    def list_members(self, group_name):
        """Return all of the members of a particular group."""
        return self.client.get_group_members(group_name)

    def list_all(self):
        """Return all of the groups in an organization."""
        return self.client.get_groups()

    def list_parents(self, member_type, member_name):
        """Retrieve all groups in which a given user or group is a member.

        member_type is either 'group' or 'user'.
        member_name is the name of the member.

        Note: this method is non-recursive - it will return all groups in
        which the given user or group is a member but not the groups in which
        those groups are members).
        """
        if member_type == "group":
            return self.client.get_groups_for_principal(group_name=member_name)
        elif member_type == "user":
            return self.client.get_groups_for_principal(user_name=member_name)
        else:
            raise GroupsException("Invalid 'member_type' {}".format(
                member_type
            ))

    def remove_member(self, parent_name, member_type, member_name):
        """Remove a user or group from a group.

        member_type is either 'group' or 'user'.
        member_name is the name of the member.
        """
        if member_type == "group":
            return self.client.remove_from_group(parent_name=parent_name,
                                                 group_name=member_name)
        elif member_type == "user":
            return self.client.remove_from_group(parent_name=parent_name,
                                                 user_name=member_name)
        else:
            raise GroupsException("Invalid 'member_type' {}".format(
                member_type
            ))

    def delete(self, group_name):
        """Remove a group from this organization."""
        return self.client.remove_group(group_name)
