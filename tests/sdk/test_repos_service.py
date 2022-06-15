# Databricks CLI
# Copyright 2022 Databricks, Inc.
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
import pytest
import mock

from databricks_cli.sdk.service import ReposService
from tests.utils import provide_conf


@pytest.fixture()
def repos_service():
    with mock.patch('databricks_cli.sdk.ApiClient') as api_client:
        yield ReposService(api_client)


def test_provider_inference():
    for url, provider in {
        "https://github.com/org/repo.git": "gitHub",
        "https://git-codecommit.us-east-2.amazonaws.com/v1/repos/MyDemoRepo": "awsCodeCommit",
        "https://github1.com/org/repo.git": None,
    }.items():
        assert provider == ReposService.detect_repo_provider(url)


@provide_conf
def test_list(repos_service):
    repos_service.list_repos()
    repos_service.client.perform_query.assert_called_with('GET', '/repos', data={}, headers=None)

    repos_service.list_repos(path_prefix='foo')
    repos_service.client.perform_query.assert_called_with('GET', '/repos', data={'path_prefix': 'foo'}, headers=None)

    repos_service.list_repos(next_page_token='token')
    repos_service.client.perform_query.assert_called_with('GET', '/repos', data={'next_page_token': 'token'}, headers=None)


@provide_conf
def test_create(repos_service):
    url = 'https://github.com/foo/bar.git'

    repos_service.create_repo(url, None)
    repos_service.client.perform_query.assert_called_with('POST', '/repos', data={'url': url, 'provider': 'gitHub'}, headers=None)

    repos_service.create_repo(url, '')
    repos_service.client.perform_query.assert_called_with('POST', '/repos', data={'url': url, 'provider': 'gitHub'}, headers=None)

    repos_service.create_repo(url, '     ')
    repos_service.client.perform_query.assert_called_with('POST', '/repos', data={'url': url, 'provider': 'gitHub'}, headers=None)

    repos_service.create_repo(url, 'gitHub')
    repos_service.client.perform_query.assert_called_with('POST', '/repos', data={'url': url, 'provider': 'gitHub'}, headers=None)

    repos_service.create_repo(url, None, path='/path')
    repos_service.client.perform_query.assert_called_with('POST', '/repos', data={'url': url, 'provider': 'gitHub', 'path': '/path'}, headers=None)


@provide_conf
def test_get(repos_service):
    repos_service.get_repo('id')
    repos_service.client.perform_query.assert_called_with('GET', '/repos/id', data={}, headers=None)


@provide_conf
def test_update(repos_service):
    repos_service.update_repo('id')
    repos_service.client.perform_query.assert_called_with('PATCH', '/repos/id', data={}, headers=None)

    repos_service.update_repo('id', branch='branch')
    repos_service.client.perform_query.assert_called_with('PATCH', '/repos/id', data={'branch': 'branch'}, headers=None)

    repos_service.update_repo('id', tag='tag')
    repos_service.client.perform_query.assert_called_with('PATCH', '/repos/id', data={'tag': 'tag'}, headers=None)

@provide_conf
def test_delete(repos_service):
    repos_service.delete_repo('id')
    repos_service.client.perform_query.assert_called_with('DELETE', '/repos/id', data={}, headers=None)
