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

from databricks_cli.sdk import ScimService
from databricks_cli.utils import is_int


class ScimError(Exception):
    pass


class ScimApi(object):
    GROUP_NAME_FILTER = 'displayName eq {}'
    USER_NAME_FILTER = 'userName eq {}'

    def __init__(self, api_client):
        self.client = ScimService(api_client)

    def get_group(self, group_id=None, group_name=None):
        if group_id is not None:
            content = self.get_group_by_id(group_id)
        else:
            filters = self.GROUP_NAME_FILTER.format(group_name)
            content = self.list_groups(filters=filters)
        return content

    def get_user(self, user_id=None, user_name=None):
        if user_id is not None:
            content = self.get_user_by_id(user_id)
        else:
            filters = self.USER_NAME_FILTER.format(user_name)
            content = self.list_users(filters=filters)
        return content

    def get_user_id_for_user(self, user_name):
        content = self.get_user(user_id=None, user_name=user_name)
        return self._parse_id_from_json(name='user', value=user_name,
                                        filters=self.USER_NAME_FILTER.format(user_name),
                                        data=content)

    def get_group_id_for_group(self, group_name):
        content = self.get_group(group_name=group_name)
        return self._parse_id_from_json(name='group', value=group_name,
                                        filters=self.GROUP_NAME_FILTER.format(group_name),
                                        data=content)

    def get_group_name_for_group(self, group_id):
        content = self.get_group(group_id=group_id)
        return self._parse_name_from_json(id_value=group_id, value=group_id, data=content)

    def delete_user(self, user_id=None, user_name=None):
        if user_id is None:
            user_id = self.get_user_id_for_user(user_name)
        content = self.delete_user_by_id(user_id)

        return content

    def list_users(self, filters=None, active=None):
        # filtering on active doesn't work:
        # https://help.databricks.com/hc/en-us/requests/24688
        content = self.client.users(filters)

        return self.filter_active_only(content, active)

    def get_user_by_id(self, user_id):
        return self.client.user_by_id(user_id)

    def create_user(self, user_name=None, groups=None, entitlements=None, roles=None):
        return self.client.create_user(user_name=user_name, groups=groups,
                                       entitlements=entitlements, roles=roles)

    def create_user_json(self, json):
        return self.client.create_user_json(data=json)

    def update_user_by_id(self, user_id, operation, path, values):
        return self.client.update_user_by_id(user_id, operation, path, values)

    def overwrite_user_by_id(self, user_id, user_name, groups, entitlements, roles):
        return self.client.overwrite_user_by_id(user_id, user_name, groups, entitlements, roles)

    def delete_user_by_id(self, user_id):
        return self.client.delete_user_by_id(user_id)

    def list_groups(self, filters=None, group_id=None, group_name=None):
        extra_filter = None
        if group_id is not None:
            # id is not supported.
            # extra_filter = 'id eq {}'.format(group_id)
            group = self.get_group(group_id=group_id)
            group_name = group.get('displayName')

        if group_name is not None:
            extra_filter = 'displayName eq {}'.format(group_name)

        if extra_filter is not None:
            if filters is not None:
                filters = filters + ' and {}'.format(extra_filter)
            else:
                filters = extra_filter

        return self.client.groups(filters)

    def get_group_by_id(self, group_id):
        return self.client.group_by_id(group_id)

    def create_group(self, group_name, users):
        user_ids = self.users_to_user_ids(users)
        return self.client.create_group_internal(group_name=group_name, members=user_ids)

    def update_group_by_id(self, group_id, operation, values):
        return self.client.update_group_by_id(group_id=group_id, operation=operation, values=values)

    def delete_group(self, group_id=None, group_name=None):
        if group_id is None:
            group_id = self.get_group_id_for_group(group_name)
        content = self.delete_group_by_id(group_id)

        return content

    def delete_group_by_id(self, group_id):
        return self.client.delete_group_by_id(group_id)

    def add_user_to_group(self, group_id, group_name, user_id, user_name):
        if group_id is None:
            group_id = self.get_group_id_for_group(group_name)
        if user_id is None:
            user_id = self.get_user_id_for_user(user_name)

        return self.group_operation(op='add', group_id=group_id, user_id=user_id)

    def remove_user_from_group(self, group_id, group_name, user_id, user_name):
        if group_id is None:
            group_id = self.get_group_id_for_group(group_name)
        if user_id is None:
            user_id = self.get_user_id_for_user(user_name)

        return self.group_operation(op='remove', group_id=group_id, user_id=[user_id])

    def group_operation(self, op, group_id, user_id):
        return self.update_group_by_id(group_id=group_id, operation=op, values=[user_id])

    @classmethod
    def users_to_user_ids(cls, users):
        """
        Convert a list of user names and or user ids to a list of user ids
        :param users: list of user names or ids
        :return: list of user ids
        """
        # we need a list of ids.
        return cls.members_to_ids(users, cls.get_user_id_for_user)

    @classmethod
    def groups_to_group_ids(cls, groups):
        """
        Convert a list of user names and or user ids to a list of user ids
        :param groups: list of user names or ids
        :return: list of user ids
        """
        # we need a list of ids.
        return cls.members_to_ids(groups, cls.get_group_id_for_group)

    @classmethod
    def filter_active_only(cls, content, active):
        resources = content.get('Resources')
        if active is not None and resources:
            content['Resources'] = [
                resource for resource in resources if resource.get('active') == active
            ]

        return content

    @classmethod
    def _parse_id_from_json(cls, name, value, filters, data):
        if not data:
            raise ScimError(
                'Failed to find {} {} using filter {}, no response'.format(name, value, filters))

        resources = data.get('Resources')
        if not resources:
            raise ScimError('Failed to find resources in json data for response: {}'.format(data))

        if len(resources) != 1:
            raise ScimError(
                'Expected only 1 resource using filter {} in json data: {}'.format(filters, data))

        resource = resources[0]
        resource_id = resource.get('id')
        if not resource_id:
            raise ScimError(
                'Expected {} id in resource using filter {} in json data: {}'.format(name,
                                                                                     filters, data))

        return resource_id

    @classmethod
    def _parse_name_from_json(cls, id_value, value, data):
        if not data:
            raise ScimError('Failed to find {} {}, no response'.format(id_value, value))

        resources = data.get('Resources')
        if not resources:
            raise ScimError('Failed to find resources in json data for response: {}'.format(data))

        resource_name = None
        for resource in resources:
            if resource.get('id') == id_value:
                resource_name = resource.get('displayName')
                break

        return resource_name

    @classmethod
    def members_to_ids(cls, members, function):
        """
        Convert a list of user names and or user ids to a list of user ids
        :param members: list of names or ids
        :param function: function to get a list of ids for a name

        :return: list of user ids
        """
        # we need a list of ids.
        list_of_ids = []
        for member in members:
            # see if it's a number
            if is_int(member):
                member_id = member
            else:
                # we need to get the user_id
                member_id = function(member)

            list_of_ids.append(member_id)

        return list_of_ids
