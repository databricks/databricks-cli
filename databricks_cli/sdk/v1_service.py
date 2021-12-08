# Databricks CLI
# Copyright 2021 Databricks, Inc.
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
import copy

from databricks_cli.sdk.version import OLD_API_VERSION


class APIV1Client(object):
    def __init__(self, client):
        self.v1_client = copy.deepcopy(client)
        self.v1_client.api_version = OLD_API_VERSION


class CommandService(APIV1Client):
    def get_command_status(self, cluster_id, context_id, command_id):
        return self.v1_client.perform_query(
            method="GET", path="/commands/status", data={
                "clusterId": cluster_id,
                "contextId": context_id,
                "commandId": command_id,
            }
        )

    def cancel_command(self, cluster_id, context_id, command_id):
        return self.v1_client.perform_query(
            method="POST", path="/commands/cancel", data={
                "clusterId": cluster_id,
                "contextId": context_id,
                "commandId": command_id,
            }
        )

    def execute_command(self, language, cluster_id, context_id, command):
        return self.v1_client.perform_query(
            method="POST", path="/commands/execute", data={
                "language": language,
                "clusterId": cluster_id,
                "contextId": context_id,
                "command": command
            }
        )


class ExecutionContextService(APIV1Client):
    def get_context_status(self, cluster_id, context_id):
        return self.v1_client.perform_query(
            method="GET", path="/contexts/status", data={
                "clusterId": cluster_id,
                "contextId": context_id,
            }
        )

    def create_context(self, language, cluster_id):
        return self.v1_client.perform_query(
            method="POST", path="/contexts/create", data={
                "language": language,
                "clusterId": cluster_id,
            }
        )

    def destroy_context(self, cluster_id, context_id):
        return self.v1_client.perform_query(
            method="POST", path="/contexts/destroy", data={
                "contextId": context_id,
                "clusterId": cluster_id,
            }
        )
