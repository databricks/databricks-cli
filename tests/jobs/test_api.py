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

CREATE_RETURN = {'job_id': 5}
CREATE_JSON = '{"name": "test_job"}'


@pytest.fixture()
def jobs_api():
    with mock.patch('databricks_cli.sdk.JobsService') as jobs_service_mock:
        jobs_service_mock.return_value = mock.MagicMock()
        jobs_api_mock = JobsApi(None)
        yield jobs_api_mock


@provide_conf
def test_get_jobs_by_name(jobs_api):
    job_name = 'test job'
    job_other_name = 'test job alt'
    test_job = {'settings': {'name': job_name}}
    test_job_alt = {'settings': {'name': job_other_name}}
    jobs_api.list_jobs = mock.MagicMock()

    jobs_api.list_jobs.return_value = {'jobs': []}
    res = jobs_api.get_jobs_by_name(job_name)
    assert len(res) == 0

    jobs_api.list_jobs.return_value = {'jobs': [test_job_alt]}
    res = jobs_api.get_jobs_by_name(job_name)
    assert len(res) == 0

    jobs_api.list_jobs.return_value = {'jobs': [test_job, test_job_alt]}
    res = jobs_api.get_jobs_by_name(job_name)
    assert len(res) == 1

    jobs_api.list_jobs.return_value = {'jobs': [test_job, test_job_alt, test_job]}
    res = jobs_api.get_jobs_by_name(job_name)
    assert len(res) == 2