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

from databricks_cli.configure.config import get_clusters_client


def create_cluster(json):
    return get_clusters_client().client.perform_query('POST', '/clusters/create', data=json)


def start_cluster(cluster_id):
    return get_clusters_client().start_cluster(cluster_id)


def restart_cluster(cluster_id):
    return get_clusters_client().restart_cluster(cluster_id)


def delete_cluster(cluster_id):
    return get_clusters_client().delete_cluster(cluster_id)


def get_cluster(cluster_id):
    return get_clusters_client().get_cluster(cluster_id)


def list_clusters():
    return get_clusters_client().list_clusters()


def list_zones():
    return get_clusters_client().list_available_zones()


def list_node_types():
    return get_clusters_client().list_node_types()


def spark_versions():
    return get_clusters_client().list_spark_versions()
