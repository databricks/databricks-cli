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

import six
from requests.exceptions import HTTPError

from databricks_cli.sdk import SecretService
from databricks_cli.utils import error_and_quit


def eat_404_exception(function):
    @six.wraps(function)
    def decorator(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except HTTPError as exception:
            if exception.response.status_code == 404:
                error_and_quit('Secrets API is not supported for this workspace.'
                               ' Please contact Databricks support.')
            else:
                raise exception
    decorator.__doc__ = function.__doc__
    return decorator


class SecretApi(object):
    def __init__(self, api_client):
        self.client = SecretService(api_client)

    @eat_404_exception
    def create_scope(self, scope, initial_manage_acl):
        return self.client.create_scope(scope, initial_manage_acl)

    @eat_404_exception
    def delete_scope(self, scope):
        return self.client.delete_scope(scope)

    @eat_404_exception
    def list_scopes(self):
        return self.client.list_scopes()

    @eat_404_exception
    def write_secret(self, scope, key, string_value, bytes_value):
        return self.client.write_secret(scope, key, string_value, bytes_value)

    @eat_404_exception
    def delete_secret(self, scope, key):
        return self.client.delete_secret(scope, key)

    @eat_404_exception
    def list_secrets(self, scope):
        return self.client.list_secrets(scope)

    @eat_404_exception
    def write_acl(self, scope, principal, permission):
        return self.client.write_acl(scope, principal, permission)

    @eat_404_exception
    def delete_acl(self, scope, principal):
        return self.client.delete_acl(scope, principal)

    @eat_404_exception
    def list_acls(self, scope):
        return self.client.list_acls(scope)

    @eat_404_exception
    def get_acl(self, scope, principal):
        return self.client.get_acl(scope, principal)