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
from databricks_cli.sdk import ClusterService


class ClusterApi(object):
    def __init__(self, api_client):
        self.client = ClusterService(api_client)

    def create_cluster(self, json):
        return self.client.client.perform_query('POST', '/clusters/create', data=json)

    def edit_cluster(self, json):
        return self.client.client.perform_query('POST', '/clusters/edit', data=json)

    def start_cluster(self, cluster_id):
        return self.client.start_cluster(cluster_id)

    def restart_cluster(self, cluster_id):
        return self.client.restart_cluster(cluster_id)

    def resize_cluster(self, cluster_id, num_workers):
        return self.client.resize_cluster(cluster_id, num_workers=num_workers)

    def delete_cluster(self, cluster_id):
        return self.client.delete_cluster(cluster_id)

    def get_cluster(self, cluster_id):
        return self.client.get_cluster(cluster_id)

    def list_clusters(self):
        return self.client.list_clusters()

    def list_zones(self):
        return self.client.list_available_zones()

    def list_node_types(self):
        return self.client.list_node_types()

    def spark_versions(self):
        return self.client.list_spark_versions()

    def permanent_delete(self, cluster_id):
        return self.client.permanent_delete_cluster(cluster_id)

    def get_events(self, cluster_id, start_time, end_time, order, event_types, offset, limit):
        return self.client.get_events(cluster_id, start_time, end_time, order, event_types,
                                      offset, limit)
