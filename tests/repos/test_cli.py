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
#
# pylint:disable=redefined-outer-name

import mock
import pytest
from click.testing import CliRunner

import databricks_cli.repos.cli as cli
from tests.utils import provide_conf


REPO_ID = '123456'
BRANCH_NAME = 'main'


@pytest.fixture()
def repos_api_mock():
    with mock.patch('databricks_cli.repos.cli.ReposApi') as ReposApiMock:
        _repos_api_mock = mock.MagicMock()
        ReposApiMock.return_value = _repos_api_mock
        yield _repos_api_mock


@provide_conf
def test_update_repos(repos_api_mock):
    runner = CliRunner()
    runner.invoke(cli.update_repo_cli, ['--repo-id', REPO_ID, '--branch', BRANCH_NAME])
    assert repos_api_mock.update.call_args[0][0] == REPO_ID
    assert repos_api_mock.update.call_args[0][1] == BRANCH_NAME

