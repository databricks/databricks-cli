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

import mock
import pytest

from databricks_cli.runs.api import RunsApi
from tests.utils import provide_conf


@pytest.fixture()
def runs_api():
    with mock.patch('databricks_cli.sdk.JobsService') as jobs_service_mock:
        jobs_service_mock.return_value = mock.MagicMock()
        runs_api_mock = RunsApi(None)
        yield runs_api_mock


@provide_conf
def test_get_run():
    with mock.patch('databricks_cli.sdk.ApiClient') as api_client_mock:
        api = RunsApi(api_client_mock)
        api.get_run('1')
        api_client_mock.perform_query.assert_called_with(
            'GET', '/jobs/runs/get', data={'run_id': '1'}, headers=None, version=None
        )

        api.get_run('1', version='3.0')
        api_client_mock.perform_query.assert_called_with(
            'GET', '/jobs/runs/get', data={'run_id': '1'}, headers=None, version='3.0'
        )


@provide_conf
def test_get_run_output():
    with mock.patch('databricks_cli.sdk.ApiClient') as api_client_mock:
        api = RunsApi(api_client_mock)
        api.get_run_output('1')
        api_client_mock.perform_query.assert_called_with(
            'GET', '/jobs/runs/get-output', data={'run_id': '1'}, headers=None, version=None
        )
        api.get_run_output('1', version='3.0')
        api_client_mock.perform_query.assert_called_with(
            'GET', '/jobs/runs/get-output', data={'run_id': '1'}, headers=None, version='3.0'
        )


@provide_conf
def test_cancel_run():
    with mock.patch('databricks_cli.sdk.ApiClient') as api_client_mock:
        api = RunsApi(api_client_mock)
        api.cancel_run('1')
        api_client_mock.perform_query.assert_called_with(
            'POST', '/jobs/runs/cancel', data={'run_id': '1'},
            headers=None, version=None
        )

        api.cancel_run('1', version='3.0')
        api_client_mock.perform_query.assert_called_with(
            'POST', '/jobs/runs/cancel', data={'run_id': '1'},
            headers=None, version='3.0'
        )


@provide_conf
def test_list_runs():
    with mock.patch('databricks_cli.sdk.ApiClient') as api_client_mock:
        api = RunsApi(api_client_mock)
        api.list_runs('1', True, False, 0, 20)
        api_client_mock.perform_query.assert_called_with(
            'GET', '/jobs/runs/list',
            data={'job_id': '1', 'active_only': True,
                  "completed_only": False, "offset": 0, "limit": 20},
            headers=None, version=None
        )

        api.list_runs('1', True, False, 0, 20, version='3.0')
        api_client_mock.perform_query.assert_called_with(
            'GET', '/jobs/runs/list',
            data={'job_id': '1', 'active_only': True,
                  "completed_only": False, "offset": 0, "limit": 20},
            headers=None, version='3.0'
        )


@provide_conf
def test_submit_run():
    with mock.patch('databricks_cli.sdk.ApiClient') as api_client_mock:
        api = RunsApi(api_client_mock)
        api.submit_run('{"tasks": [], "run_name": "mock"}', version=None)
        api_client_mock.perform_query.assert_called_with(
            'POST', '/jobs/runs/submit', data='{"tasks": [], "run_name": "mock"}',
            version=None
        )

        api.submit_run('{"tasks": [], "run_name": "mock"}', version='3.0')
        api_client_mock.perform_query.assert_called_with(
            'POST', '/jobs/runs/submit', data='{"tasks": [], "run_name": "mock"}',
            version='3.0'
        )
