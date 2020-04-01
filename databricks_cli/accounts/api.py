"""Implement Databricks Accounts API, interfacing with the AccountsService."""
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
from databricks_cli.sdk import AccountsService


class AccountsApi(object):
    """Implement the databricks '2.0/accounts' API Interface."""

    def __init__(self, api_client):
        self.client = AccountsService(api_client)

    def create_credentials(self, account_id, json):
        """Create a new credentials object with AWS IAM role reference."""
        if account_id is None:
            raise TypeError('Expected account_id')
        endpoint = "/accounts/%s/credentials" % (account_id)
        return self.client.client.perform_query('POST', endpoint, data=json)

    def get_credentials(self, account_id, credentials_id):
        """Get the credentials object with the given credentials id."""
        return self.client.get_credentials(account_id, credentials_id)

    def delete_credentials(self, account_id, credentials_id):
        """Delete the credentials object for the given credentials id."""
        return self.client.delete_credentials(account_id, credentials_id)

    def create_storage_config(self, account_id, json):
        """Create a new storage config object with AWS bucket reference."""
        if account_id is None:
            raise TypeError('Expected account_id')
        endpoint = "/accounts/%s/storage-configurations" % (account_id)
        return self.client.client.perform_query('POST', endpoint, data=json)

    def get_storage_config(self, account_id, storage_config_id):
        """Get the storage config object with the given storage config id."""
        return self.client.get_storage_config(account_id, storage_config_id)

    def delete_storage_config(self, account_id, storage_config_id):
        """Delete the storage config object for the given storage config id."""
        return self.client.delete_storage_config(account_id, storage_config_id)

    def create_network(self, account_id, json):
        """Create a new network object with AWS network infrastructure reference."""
        if account_id is None:
            raise TypeError('Expected account_id')
        endpoint = "/accounts/%s/networks" % (account_id)
        return self.client.client.perform_query('POST', endpoint, data=json)

    def get_network(self, account_id, network_id):
        """Get the network object with the given network id."""
        return self.client.get_network(account_id, network_id)

    def delete_network(self, account_id, network_id):
        """Delete the network object for the given network id."""
        return self.client.delete_network(account_id, network_id)

    def create_workspace(self, account_id, json):
        """Create a new workspace with required references."""
        if account_id is None:
            raise TypeError('Expected account_id')
        endpoint = "/accounts/%s/workspaces" % (account_id)
        return self.client.client.perform_query('POST', endpoint, data=json)

    def get_workspace(self, account_id, workspace_id):
        """Get the workspace details with the given workspace id."""
        return self.client.get_workspace(account_id, workspace_id)

    def delete_workspace(self, account_id, workspace_id):
        """Delete the workspace for the given workspace id."""
        return self.client.delete_workspace(account_id, workspace_id)