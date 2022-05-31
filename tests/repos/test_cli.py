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

# pylint:disable=redefined-outer-name

import mock
import pytest
from click.testing import CliRunner

import databricks_cli.repos.cli as cli
from tests.utils import provide_conf

TEST_PATH_PREFIX = "/Repos/"
TEST_NEXT_PAGE_TOKEN = "eyJyZXBvX3RyZWVub2RlX2lkIjo1MjQ5NjA4ODE0NTA5Mjc5fQ=="
TEST_URL = "https://github.com/my/my-repo"
TEST_PROVIDER = "github"
TEST_PATH = "/Repos/someone@example.com/my-repo"
TEST_BRANCH = "main"
TEST_TAG = "v1.0"
TEST_HEAD_COMMIT_ID = "9837ac1f924a5ca56117597c5c79bb02300ff1f4"
TEST_ID = "1234567890123456"


@pytest.fixture()
def repos_api_mock():
    with mock.patch('databricks_cli.repos.cli.ReposApi') as mocked:
        _repos_api_mock = mock.MagicMock()
        mocked.return_value = _repos_api_mock
        yield _repos_api_mock


@provide_conf
def test_list_validation(repos_api_mock):
    runner = CliRunner()
    runner.invoke(cli.list_repos_cli,
                  ["--path-prefix", TEST_PATH_PREFIX,
                   "--next-page-token", TEST_NEXT_PAGE_TOKEN])
    repos_api_mock.list.assert_called_once_with(
        TEST_PATH_PREFIX,
        TEST_NEXT_PAGE_TOKEN,
    )


@provide_conf
def test_create_validation(repos_api_mock):
    runner = CliRunner()
    runner.invoke(cli.create_repo_cli,
                  ["--url", TEST_URL,
                   "--provider", TEST_PROVIDER,
                   "--path", TEST_PATH])
    repos_api_mock.create.assert_called_once_with(
        TEST_URL,
        TEST_PROVIDER,
        TEST_PATH,
    )


@provide_conf
def test_create_validation_partial(repos_api_mock):
    runner = CliRunner()
    runner.invoke(cli.create_repo_cli,
                  ["--url", TEST_URL])
    repos_api_mock.create.assert_called_once_with(
        TEST_URL,
        None,
        None,
    )


@provide_conf
def test_get_validation(repos_api_mock):
    runner = CliRunner()
    runner.invoke(cli.get_repo_cli,
                  ["--repo-id", TEST_ID])
    repos_api_mock.get.assert_called_once_with(
        TEST_ID,
    )


@provide_conf
def test_get_id_validation(repos_api_mock):
    runner = CliRunner()
    runner.invoke(cli.get_repo_cli,
                  ["--path", TEST_PATH])
    repos_api_mock.get_repo_id.assert_called_once_with(
        TEST_PATH,
    )


@provide_conf
def test_update_branch_validation(repos_api_mock):
    runner = CliRunner()
    runner.invoke(cli.update_repo_cli,
                  ["--repo-id", TEST_ID,
                   "--branch", TEST_BRANCH])
    repos_api_mock.update.assert_called_once_with(
        TEST_ID,
        TEST_BRANCH,
        None
    )


@provide_conf
def test_update_tag_validation(repos_api_mock):
    runner = CliRunner()
    runner.invoke(cli.update_repo_cli,
                  ["--repo-id", TEST_ID,
                   "--tag", TEST_TAG])
    repos_api_mock.update.assert_called_once_with(
        TEST_ID,
        None,
        TEST_TAG
    )


@provide_conf
def test_update_parameter_validation(repos_api_mock):
    runner = CliRunner()
    runner.invoke(cli.update_repo_cli,
                  ["--repo-id", TEST_ID,
                   "--branch", TEST_BRANCH,
                   "--tag", TEST_TAG])
    # not called because only one of branch or tag should be specified
    assert repos_api_mock.update.call_count == 0


@provide_conf
def test_delete_validation(repos_api_mock):
    runner = CliRunner()
    runner.invoke(cli.delete_repo_cli,
                  ["--repo-id", TEST_ID])
    repos_api_mock.delete.assert_called_once_with(
        TEST_ID,
    )
