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
import mock
import pytest

from databricks_cli.commands.api import CommandApi, ExecutionContextApi


@pytest.fixture(name="command_api")
def command_api_fixture():
    with mock.patch('databricks_cli.sdk.ApiClient') as api_client:
        yield CommandApi(api_client)


def test_execute_command_until_terminated(command_api):
    command_api.get_command_status = mock.Mock(
        side_effect=[{"status": "Running"}, {"status": "Finished"}]
    )
    command_api.execute_command_until_terminated(
        cluster_id=mock.ANY, context_id=mock.ANY, command=mock.ANY, language=mock.ANY)
    command_api.v1_client.perform_query \
        .assert_called_with(method="POST", path="/commands/execute",
                            data={
                                'contextId': mock.ANY, 'clusterId': mock.ANY,
                                'command': mock.ANY, 'language': mock.ANY
                            })
    assert command_api.get_command_status.call_count == 2


@pytest.fixture(name="exec_ctx_api")
def exec_ctx_api_fixture():
    with mock.patch('databricks_cli.sdk.ApiClient') as api_client:
        yield ExecutionContextApi(api_client)


def test_create_context(exec_ctx_api):
    with mock.patch("databricks_cli.commands.api.ExecutionContextService.create_context") \
            as cc_mock:
        cc_mock.side_effect = [RuntimeError("error"), {"status": "success"}]
        exec_ctx_api.create_context(language=mock.ANY, cluster_id=mock.ANY)
        assert cc_mock.call_count == 2
