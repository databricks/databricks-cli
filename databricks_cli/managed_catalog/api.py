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
from databricks_cli.sdk import ManagedCatalogService


class ManagedCatalogApi(object):
    def __init__(self, api_client):
        self.client = ManagedCatalogService(api_client)

    def create_metastore(self, name, storage_root):
        return self.client.create_metastore(name, storage_root)

    def list_metastores(self):
        return self.client.list_metastores()

    def get_metastore(self, metastore_id):
        return self.client.get_metastore(metastore_id)

    def update_metastore(self, metastore_id, metastore_spec):
        return self.client.update_metastore(metastore_id, metastore_spec)

    def delete_metastore(self, metastore_id):
        return self.client.delete_metastore(metastore_id)

    def create_catalog(self, catalog_name, comment):
        return self.client.create_catalog(catalog_name, comment)

    def delete_catalog(self, catalog_name):
        return self.client.delete_catalog(catalog_name)

    def create_schema(self, catalog_name, schema_name, comment):
        return self.client.create_schema(catalog_name, schema_name, comment)

    def delete_schema(self, schema_full_name):
        return self.client.delete_schema(schema_full_name)

    def create_table(self, table_spec):
        return self.client.create_table(table_spec)

    def delete_table(self, table_full_name):
        return self.client.delete_table(table_full_name)

    def create_dac(self, dac):
        return self.client.create_dac(dac)

    def get_dac(self, dac_id):
        return self.client.get_dac(dac_id)

    def create_root_credentials(self, root_creds):
        return self.client.create_root_credentials(root_creds)
