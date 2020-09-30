#
# Databricks CLI
# Copyright 2020 Databricks, Inc.
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
#
from databricks_cli.utils import debug


class ScimService(object):
    ACCEPT_TYPE = 'application/scim+json'
    PREVIEW_BASE = '/preview/scim/v2/'

    FILTERS = {
        # Operator: Description, # Behavior
        "eq": "equals",  # Attribute and operator values must be identical.
        "ne": "not_equal",  # equal to Attribute and operator values are not identical.
        "co": "contains",  # Operator value must be a substring of attribute value.
        "sw": "starts_with",  # with Attribute must start with and contain operator value.
        "and": "and",  # AND Match when all expressions evaluate to true.
        "or": "or",  # OR Match when any expression evaluates to true.
    }

    def __init__(self, client):
        self.client = client

    @classmethod
    def _add_accept(cls, headers):
        if headers is None:
            headers = {}
        headers.update({'Accept': 'application/scim+json'})
        return headers

    def users(self, filters=None, headers=None):
        headers = self._add_accept(headers)
        if filters is not None:
            filters = '?filter=' + filters
        else:
            filters = ''
        return self.client.perform_query('GET', '{}Users{}'.format(self.PREVIEW_BASE, filters), headers=headers)

    def user_by_id(self, user_id, headers=None):
        headers = self._add_accept(headers)
        return self.client.perform_query('GET', '{}Users/{}'.format(self.PREVIEW_BASE, user_id), headers=headers)

    @classmethod
    def _update_user_data(cls, _data, groups, entitlements, roles):

        if groups is not None:
            _data.update({'groups': cls.ids_list_to_values_list(groups)})
        if entitlements is not None:
            _data.update({'entitlements': cls.ids_list_to_values_list(entitlements)})
        if roles is not None:
            _data.update({'roles': cls.ids_list_to_values_list(roles)})

        return _data

    def create_user(self, user_name, groups, entitlements, roles, headers=None):
        headers = self._add_accept(headers)
        _data = self._update_user_data({
            'schemas': [
                'urn:ietf:params:scim:schemas:core:2.0:User'
            ],
            'userName': user_name,
        }, groups=groups, entitlements=entitlements, roles=roles)

        return self.create_user_json(_data, headers)

    def create_user_json(self, data, headers=None):
        headers = self._add_accept(headers)
        return self.client.perform_query('POST', '{}Users'.format(self.PREVIEW_BASE), data=data, headers=headers)

    def update_user_by_id(self, user_id, operation, path, values, headers=None):
        headers = self._add_accept(headers)
        _data = {
            'schemas': [
                'urn:ietf:params:scim:api:messages:2.0:PatchOp'
            ],
            'op': operation,
            'path': path,
            'value': self.ids_list_to_values_list(values)
        }
        return self.client.perform_query('PATCH', '{}Users/{}'.format(self.PREVIEW_BASE, user_id), data=_data,
                                         headers=headers)

    def overwrite_user_by_id(self, user_id, user_name, groups, entitlements, roles, headers=None):
        headers = self._add_accept(headers)

        _data = self._update_user_data({
            'schemas': [
                'urn:ietf:params:scim:schemas:core:2.0:User'
            ],
            'userName': user_name,
        }, groups=groups, entitlements=entitlements, roles=roles)

        return self.client.perform_query('PUT', '{}Users/{}'.format(self.PREVIEW_BASE, user_id), data=_data,
                                         headers=headers)

    def delete_user_by_id(self, user_id, headers=None):
        headers = self._add_accept(headers)
        return self.client.perform_query('DELETE', '{}Users/{}'.format(self.PREVIEW_BASE, user_id), headers=headers)

    def groups(self, filters=None, headers=None):
        headers = self._add_accept(headers)
        if filters is not None:
            if isinstance(filters, list):
                filters = ' '.join(filters)
            filters = '?filter=' + filters
        else:
            filters = ''
        return self.client.perform_query('GET', '{}Groups{}'.format(self.PREVIEW_BASE, filters), headers=headers)

    def group_by_id(self, group_id, headers=None):
        headers = self._add_accept(headers)
        return self.client.perform_query('GET', '{}Groups/{}'.format(self.PREVIEW_BASE, group_id), headers=headers)

    def create_group_internal(self, group_name, members, headers=None):
        headers = self._add_accept(headers)

        _data = {
            'schemas': [
                'urn:ietf:params:scim:schemas:core:2.0:Group'
            ],
            'displayName': group_name,
            # members is a list of ids, we need to convert it to a list of hashes.
            'members': self.ids_list_to_values_list(members)
        }

        return self.client.perform_query('POST', '{}Groups'.format(self.PREVIEW_BASE), data=_data, headers=headers)

    def update_group_by_id(self, group_id, operation, values, headers=None):
        headers = self._add_accept(headers)

        operation_data = {
            'op': operation,
            'value': {
                'members': self.ids_list_to_values_list(values)
            }
        }

        # https://help.databricks.com/hc/en-us/requests/24702 remove requires a path
        if operation == 'remove':
            operation_data['path'] = 'members'

        _data = {
            'schemas': [
                'urn:ietf:params:scim:api:messages:2.0:PatchOp'
            ],
            'Operations': [operation_data]
        }

        debug('update_group_by_id', 'data: {}'.format(_data))
        return self.client.perform_query('PATCH', '{}Groups/{}'.format(self.PREVIEW_BASE, group_id), data=_data,
                                         headers=headers)

    def delete_group_by_id(self, group_id, headers=None):
        headers = self._add_accept(headers)
        return self.client.perform_query('DELETE', '{}Groups/{}'.format(self.PREVIEW_BASE, group_id), headers=headers)

    @classmethod
    def ids_list_to_values_list(cls, ids_list):
        return [{'value': id_value} for id_value in ids_list]
