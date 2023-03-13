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

from databricks_cli.jobs.api import JobsApi
from tests.utils import provide_conf


@pytest.fixture()
def jobs_api():
    with mock.patch('databricks_cli.sdk.JobsService') as jobs_service_mock:
        jobs_service_mock.return_value = mock.MagicMock()
        jobs_api_mock = JobsApi(None)
        yield jobs_api_mock


@provide_conf
def test_list_jobs_by_name(jobs_api):
    test_job_name = 'test job'
    test_job_alt_name = 'test job alt'
    test_job = {'settings': {'name': test_job_name}}
    test_job_alt = {'settings': {'name': test_job_alt_name}}
    jobs_api.list_jobs = mock.MagicMock()

    jobs_api.list_jobs.return_value = {'jobs': []}
    res = jobs_api._list_jobs_by_name(test_job_name)
    assert len(res) == 0

    jobs_api.list_jobs.return_value = {'jobs': [test_job_alt]}
    res = jobs_api._list_jobs_by_name(test_job_name)
    assert len(res) == 0

    jobs_api.list_jobs.return_value = {'jobs': [test_job, test_job_alt]}
    res = jobs_api._list_jobs_by_name(test_job_name)
    assert len(res) == 1
    assert res[0]['settings']['name'] == test_job_name

    jobs_api.list_jobs.return_value = {'jobs': [test_job, test_job_alt, test_job]}
    res = jobs_api._list_jobs_by_name(test_job_name)
    assert len(res) == 2
    assert res[0]['settings']['name'] == test_job_name
    assert res[1]['settings']['name'] == test_job_name


@provide_conf
def test_delete_job():
    with mock.patch('databricks_cli.sdk.ApiClient') as api_client_mock:
        api = JobsApi(api_client_mock)
        api.delete_job('1')
        api_client_mock.perform_query.assert_called_with(
            'POST', '/jobs/delete', data={'job_id': '1'}, headers=None, version=None
        )
        api.delete_job('1', version='3.0')
        api_client_mock.perform_query.assert_called_with(
            'POST', '/jobs/delete', data={'job_id': '1'}, headers=None, version='3.0'
        )


@provide_conf
def test_get_job():
    with mock.patch('databricks_cli.sdk.ApiClient') as api_client_mock:
        api = JobsApi(api_client_mock)
        api.get_job('1')
        api_client_mock.perform_query.assert_called_with(
            'GET', '/jobs/get', data={'job_id': '1'}, headers=None, version=None
        )

        api.get_job('1', version='3.0')
        api_client_mock.perform_query.assert_called_with(
            'GET', '/jobs/get', data={'job_id': '1'}, headers=None, version='3.0'
        )


@provide_conf
def test_reset_job():
    with mock.patch('databricks_cli.sdk.ApiClient') as api_client_mock:
        api = JobsApi(api_client_mock)
        api.reset_job({'job_id': '1', 'name': 'new_name'})
        api_client_mock.perform_query.assert_called_with(
            'POST', '/jobs/reset', data={'job_id': '1', 'name': 'new_name'},
            headers=None, version=None
        )

        api.reset_job({'job_id': '1', 'name': 'new_name'}, version='3.0')
        api_client_mock.perform_query.assert_called_with(
            'POST', '/jobs/reset', data={'job_id': '1', 'name': 'new_name'},
            headers=None, version='3.0'
        )


@provide_conf
def test_list_jobs():
    with mock.patch('databricks_cli.sdk.ApiClient') as api_client_mock:
        api = JobsApi(api_client_mock)
        api.list_jobs()
        api_client_mock.perform_query.assert_called_with(
            'GET', '/jobs/list', data={}, headers=None, version=None
        )

        api.list_jobs(version='3.0')
        api_client_mock.perform_query.assert_called_with(
            'GET', '/jobs/list', data={}, headers=None, version='3.0'
        )

        api.list_jobs(version='2.1', name='foo')
        api_client_mock.perform_query.assert_called_with(
            'GET', '/jobs/list', data={'name':'foo'}, headers=None, version='2.1'
        )


@provide_conf
def test_run_now():
    with mock.patch('databricks_cli.sdk.ApiClient') as api_client_mock:
        api = JobsApi(api_client_mock)
        api.run_now('1', ['bla'], None, None, None, None)
        api_client_mock.perform_query.assert_called_with(
            'POST', '/jobs/run-now', data={'job_id': '1', 'jar_params': ['bla']},
            headers=None, version=None
        )

        api.run_now('1', None, None, None, None, None, 'idempotent-token')
        api_client_mock.perform_query.assert_called_with(
            'POST', '/jobs/run-now', data={'job_id': '1', 'idempotency_token': 'idempotent-token'},
            headers=None, version=None
        )

        api.run_now('1', ['bla'], None, None, None, None, version='3.0')
        api_client_mock.perform_query.assert_called_with(
            'POST', '/jobs/run-now', data={'job_id': '1', 'jar_params': ['bla']},
            headers=None, version='3.0'
        )
