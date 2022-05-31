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
import json

import mock

import pytest
import requests

import databricks_cli.repos.api as api
import databricks_cli.workspace.api as workspace_api

TEST_PATH = "/Repos/someone@example.com/my-repo"
TEST_ID = "1234567890123456"
TEST_JSON_RESPONSE = {
    'path': TEST_PATH,
    'object_type': workspace_api.REPO,
    'created_at': 124,
    'object_id': TEST_ID,
}
TEST_NON_REPO_JSON_RESPONSE = {
    'path': TEST_PATH,
    'object_type': workspace_api.DIRECTORY,
    'created_at': 124,
    'object_id': TEST_ID,
}
TEST_ERROR_JSON_RESPONSE = {
    'error': "Permission Denied",
    'message': "Unauthorized access to Org: 123456",
}


@pytest.fixture()
def repos_api_with_ws_service():
    with mock.patch('databricks_cli.repos.api.WorkspaceService') as WorkspaceServiceMock:
        WorkspaceServiceMock.return_value = mock.MagicMock()
        repos_api_with_ws_service = api.ReposApi(None)
        yield repos_api_with_ws_service


class TestGetRepoId(object):
    def test_get_repo_id(self, repos_api_with_ws_service):
        repos_api_with_ws_service.ws_client.get_status.return_value = TEST_JSON_RESPONSE
        assert repos_api_with_ws_service.get_repo_id(TEST_PATH) == TEST_ID

    def test_get_id_wrong_path(self, repos_api_with_ws_service):
        repos_api_with_ws_service.ws_client.get_status.return_value = TEST_JSON_RESPONSE
        with pytest.raises(ValueError):
            repos_api_with_ws_service.get_repo_id("/Repos/BadPath")

    def test_get_id_path_not_in_repos(self, repos_api_with_ws_service):
        repos_api_with_ws_service.ws_client.get_status.return_value = TEST_JSON_RESPONSE
        with pytest.raises(ValueError):
            repos_api_with_ws_service.get_repo_id("/Bad/Path/Repo")

    def test_get_id_not_a_repo(self, repos_api_with_ws_service):
        repos_api_with_ws_service.ws_client.get_status.return_value = TEST_NON_REPO_JSON_RESPONSE
        with pytest.raises(RuntimeError):
            repos_api_with_ws_service.get_repo_id(TEST_PATH)

    def test_get_id_other_error(self, repos_api_with_ws_service):
        response = requests.Response()
        response.status_code = 403
        response._content = json.dumps(TEST_ERROR_JSON_RESPONSE).encode() #  NOQA
        repos_api_with_ws_service.ws_client.get_status.side_effect = mock.Mock(
            side_effect=requests.exceptions.HTTPError(response=response))
        with pytest.raises(RuntimeError):
            repos_api_with_ws_service.get_repo_id(TEST_PATH)


class TestCreateRepo(object):
    def test_get(self, repos_api_with_ws_service):
        with pytest.raises(ValueError):
            repos_api_with_ws_service.create("https://github1.com/user/repo.git", None, None)