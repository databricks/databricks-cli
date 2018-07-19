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

import databricks_cli.stack.cli as cli
from tests.utils import provide_conf


@pytest.fixture()
def stack_api_mock():
    with mock.patch('databricks_cli.workspace.cli.StackApi') as StackApiMock:
        stack_api_mock = mock.MagicMock()
        StackApiMock.return_value = stack_api_mock
        yield stack_api_mock


@provide_conf
def test_deploy(stack_api_mock, tmpdir):
    path = tmpdir.strpath
    stack_api_mock.deploy = mock.MagicMock()
    runner = CliRunner()
    runner.invoke(cli.deploy, ['--overwrite-notebooks', path])
    assert stack_api_mock.call_args[0][1] == os.path.join(
        path, 'notebook-name.scala')
