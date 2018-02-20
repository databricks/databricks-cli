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

from databricks_cli.configure.config import get_libraries_client


def all_cluster_statuses():
    return get_libraries_client().all_cluster_statuses()


def cluster_status(cluster_id):
    return get_libraries_client().cluster_status(cluster_id)


def install_libraries(cluster_id, libraries):
    return get_libraries_client().install_libraries(cluster_id, libraries)


def uninstall_libraries(cluster_id, libraries):
    return get_libraries_client().uninstall_libraries(cluster_id, libraries)
