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
from tenacity import stop_after_attempt, retry, wait_random_exponential, retry_if_result

from databricks_cli.sdk.v1_service import CommandService, ExecutionContextService


def command_is_not_terminated(resp):
    return resp["status"] not in {"Finished", "Cancelled", "Error"}


class CommandApi(CommandService):
    @retry(retry=retry_if_result(command_is_not_terminated),
           wait=wait_random_exponential(multiplier=1, max=30))
    def wait_command_until_terminated(self, cluster_id, context_id, command_id):
        return self.get_command_status(cluster_id, context_id, command_id)

    def execute_command_until_terminated(self, language, cluster_id, context_id, command):
        resp = self.execute_command(language=language, cluster_id=cluster_id,
                                    context_id=context_id, command=command)
        command_id = resp["id"]
        return self.wait_command_until_terminated(cluster_id=cluster_id,
                                                  context_id=context_id, command_id=command_id)


class ExecutionContextApi(ExecutionContextService):
    # Sometimes cluster is already in the status="RUNNING", however it couldn't provide the
    # execution context to make the execute command stable yet.
    @retry(stop=stop_after_attempt(10), wait=wait_random_exponential(multiplier=5, max=30))
    def create_context(self, language, cluster_id):
        return super().create_context(language, cluster_id)
