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
from databricks_cli.sdk import UnityCatalogService


class UnityCatalogApi(object):
    def __init__(self, api_client):
        self.client = UnityCatalogService(api_client)

    # Metastore APIs

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

    def get_metastore_summary(self):
        return self.client.get_metastore_summary()

    # Catalog APIs

    def create_catalog(self, catalog_name, comment):
        return self.client.create_catalog(catalog_name, comment)

    def list_catalogs(self):
        return self.client.list_catalogs()

    def get_catalog(self, name):
        return self.client.get_catalog(name)

    def update_catalog(self, name, catalog_spec):
        return self.client.update_catalog(name, catalog_spec)

    def delete_catalog(self, catalog_name):
        return self.client.delete_catalog(catalog_name)

    # Schema APIs

    def create_schema(self, catalog_name, schema_name, comment):
        return self.client.create_schema(catalog_name, schema_name, comment)

    def list_schemas(self, catalog_name, name_regex):
        return self.client.list_schemas(catalog_name, name_regex)

    def get_schema(self, full_name):
        return self.client.get_schema(full_name)

    def update_schema(self, full_name, schema_spec):
        return self.client.update_schema(full_name, schema_spec)

    def delete_schema(self, schema_full_name):
        return self.client.delete_schema(schema_full_name)

    # Table APIs

    def create_table(self, table_spec):
        return self.client.create_table(table_spec)

    def list_tables(self, catalog_name, schema_name, name_regex):
        return self.client.list_tables(catalog_name, schema_name, name_regex)

    def get_table(self, full_name):
        return self.client.get_table(full_name)

    def update_table(self, full_name, table_spec):
        return self.client.update_table(full_name, table_spec)

    def delete_table(self, table_full_name):
        return self.client.delete_table(table_full_name)

    # Data Access Configuration APIs

    def create_dac(self, metastore_id, dac):
        return self.client.create_dac(metastore_id, dac)

    def list_dacs(self, metastore_id):
        return self.client.list_dacs(metastore_id)

    def get_dac(self, metastore_id, dac_id):
        return self.client.get_dac(metastore_id, dac_id)

    def delete_dac(self, metastore_id, dac_id):
        return self.client.delete_dac(metastore_id, dac_id)

    def create_root_credentials(self, root_creds):
        return self.client.create_root_credentials(root_creds)

    # Permissions APIs

    def get_permissions(self, catalog_name, schema_full_name, table_full_name,
                        share_name):
        return self.client.get_permissions(catalog_name, schema_full_name,
                                           table_full_name, share_name)

    def update_permissions(self, catalog_name, schema_full_name, table_full_name,
                           share_name, diff_spec):
        return self.client.update_permissions(catalog_name, schema_full_name, table_full_name,
                                              share_name, diff_spec)

    # Share not supported for replace_permissions API
    def replace_permissions(self, catalog_name, schema_full_name, table_full_name, perm_spec):
        return self.client.replace_permissions(catalog_name, schema_full_name, table_full_name,
                                               perm_spec)
    # Share APIs

    def create_share(self, name):
        return self.client.create_share(name)

    def list_shares(self):
        return self.client.list_shares()

    def get_share(self, name):
        return self.client.get_share(name)

    def update_share(self, name, share_spec):
        return self.client.update_share(name, share_spec)

    def delete_share(self, name):
        return self.client.delete_share(name)

    # Recipient APIs

    def create_recipient(self, name, comment):
        return self.client.create_recipient(name, comment)

    def list_recipients(self):
        return self.client.list_recipients()

    def get_recipient(self, name):
        return self.client.get_recipient(name)

    def delete_recipient(self, name):
        return self.client.delete_recipient(name)
