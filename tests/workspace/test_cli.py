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

import os
import mock
import pytest
from click.testing import CliRunner

import databricks_cli.workspace.cli as cli
from databricks_cli.workspace.api import WorkspaceFileInfo, NOTEBOOK
from databricks_cli.workspace.types import WorkspaceLanguage
from tests.utils import provide_conf


@pytest.fixture()
def workspace_api_mock():
    with mock.patch('databricks_cli.workspace.cli.WorkspaceApi') as WorkspaceApiMock:
        workspace_api_mock = mock.MagicMock()
        WorkspaceApiMock.return_value = workspace_api_mock
        yield workspace_api_mock


@provide_conf
def test_export_workspace_cli(workspace_api_mock, tmpdir):
    path = tmpdir.strpath
    workspace_api_mock.get_status.return_value = \
        WorkspaceFileInfo('/notebook-name', NOTEBOOK, '22', WorkspaceLanguage.SCALA)
    runner = CliRunner()
    runner.invoke(cli.export_workspace_cli, ['--format', 'SOURCE', '/notebook-name', path])
    assert workspace_api_mock.export_workspace.call_args[0][1] == os.path.join(
        path, 'notebook-name.scala')
