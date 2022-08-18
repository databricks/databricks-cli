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
import pytest
import mock

from databricks_cli.sdk.service import JobsService
from tests.utils import provide_conf


@pytest.fixture()
def jobs_service():
    with mock.patch('databricks_cli.sdk.ApiClient') as api_client:
        yield JobsService(api_client)

@provide_conf
def test_delete_job(jobs_service):
    jobs_service.delete_job(None)
    jobs_service.client.perform_query.assert_called_with('POST', '/jobs/delete', data={}, headers=None, version=None)

    jobs_service.delete_job(1)
    jobs_service.client.perform_query.assert_called_with('POST', '/jobs/delete', data={'job_id': 1}, headers=None, version=None)

    jobs_service.delete_job(1, version='2.1')
    jobs_service.client.perform_query.assert_called_with('POST', '/jobs/delete', data={'job_id': 1}, headers=None, version='2.1')


@provide_conf
def test_get_job(jobs_service):
    jobs_service.get_job(None)
    jobs_service.client.perform_query.assert_called_with('GET', '/jobs/get', data={}, headers=None, version=None)

    jobs_service.get_job(1)
    jobs_service.client.perform_query.assert_called_with('GET', '/jobs/get', data={'job_id': 1}, headers=None, version=None)

    jobs_service.get_job(1, version='2.1')
    jobs_service.client.perform_query.assert_called_with('GET', '/jobs/get', data={'job_id': 1}, headers=None, version='2.1')


@provide_conf
def test_update_job(jobs_service):
    jobs_service.update_job(None)
    jobs_service.client.perform_query.assert_called_with('POST', '/jobs/update', data={}, headers=None, version=None)

    jobs_service.update_job(1)
    jobs_service.client.perform_query.assert_called_with('POST', '/jobs/update', data={'job_id': 1}, headers=None, version=None)

    jobs_service.update_job(1, version='2.1')
    jobs_service.client.perform_query.assert_called_with('POST', '/jobs/update', data={'job_id': 1}, headers=None, version='2.1')

    # new_settings_argument
    new_settings = {
        "name": "job1",
        "tags": {"cost-center": "engineering","team": "jobs"}
    }
    jobs_service.update_job(1, version='2.1', new_settings=new_settings)
    jobs_service.client.perform_query.assert_called_with('POST', '/jobs/update', data={'job_id': 1, 'new_settings': new_settings}, headers=None, version='2.1')

    # fields_to_remove argument
    fields_to_remove = ["libraries", "schedule"]
    jobs_service.update_job(1, version='2.1', fields_to_remove=fields_to_remove)
    jobs_service.client.perform_query.assert_called_with('POST', '/jobs/update', data={'job_id': 1, 'fields_to_remove': fields_to_remove}, headers=None, version='2.1')



@provide_conf
def test_list_jobs(jobs_service):
    jobs_service.list_jobs()
    jobs_service.client.perform_query.assert_called_with('GET', '/jobs/list', data={}, headers=None, version=None)

    jobs_service.list_jobs(offset=1, limit=1)
    jobs_service.client.perform_query.assert_called_with('GET', '/jobs/list', data={'offset': 1, 'limit': 1}, headers=None, version=None)

    jobs_service.list_jobs(expand_tasks=True, version='2.1')
    jobs_service.client.perform_query.assert_called_with('GET', '/jobs/list', data={'expand_tasks': True}, headers=None, version='2.1')


@provide_conf
def test_get_run_output(jobs_service):
    jobs_service.get_run_output(None)
    jobs_service.client.perform_query.assert_called_with('GET', '/jobs/runs/get-output', data={}, headers=None, version=None)

    jobs_service.get_run_output(1)
    jobs_service.client.perform_query.assert_called_with('GET', '/jobs/runs/get-output', data={'run_id': 1}, headers=None, version=None)

    jobs_service.get_run_output(1, version='2.1')
    jobs_service.client.perform_query.assert_called_with('GET', '/jobs/runs/get-output', data={'run_id': 1}, headers=None, version='2.1')


@provide_conf
def test_get_run(jobs_service):
    jobs_service.get_run(None)
    jobs_service.client.perform_query.assert_called_with('GET', '/jobs/runs/get', data={}, headers=None, version=None)

    jobs_service.get_run(1)
    jobs_service.client.perform_query.assert_called_with('GET', '/jobs/runs/get', data={'run_id': 1}, headers=None, version=None)

    jobs_service.get_run(1, version='2.1')
    jobs_service.client.perform_query.assert_called_with('GET', '/jobs/runs/get', data={'run_id': 1}, headers=None, version='2.1')


@provide_conf
def test_delete_run(jobs_service):
    jobs_service.delete_run(None)
    jobs_service.client.perform_query.assert_called_with('POST', '/jobs/runs/delete', data={}, headers=None, version=None)

    jobs_service.delete_run(1)
    jobs_service.client.perform_query.assert_called_with('POST', '/jobs/runs/delete', data={'run_id': 1}, headers=None, version=None)

    jobs_service.delete_run(1, version='2.1')
    jobs_service.client.perform_query.assert_called_with('POST', '/jobs/runs/delete', data={'run_id': 1}, headers=None, version='2.1')


@provide_conf
def test_cancel_run(jobs_service):
    jobs_service.cancel_run(None)
    jobs_service.client.perform_query.assert_called_with('POST', '/jobs/runs/cancel', data={}, headers=None, version=None)

    jobs_service.cancel_run(1)
    jobs_service.client.perform_query.assert_called_with('POST', '/jobs/runs/cancel', data={'run_id': 1}, headers=None, version=None)

    jobs_service.cancel_run(1, version='2.1')
    jobs_service.client.perform_query.assert_called_with('POST', '/jobs/runs/cancel', data={'run_id': 1}, headers=None, version='2.1')


@provide_conf
def test_create_job(jobs_service):
    jobs_service.create_job(None)
    jobs_service.client.perform_query.assert_called_with('POST', '/jobs/create', data={}, headers=None, version=None)

    tasks = {'task_key': '123', 'notebook_task': {'notebook_path': '/test'}}
    jobs_service.create_job(tasks=tasks)
    jobs_service.client.perform_query.assert_called_with('POST', '/jobs/create', data={'tasks': tasks}, headers=None, version=None)

    tasks = {'task_key': '123', 'notebook_task': {'notebook_path': '/test'}}
    jobs_service.create_job(tasks=tasks, version='2.1')
    jobs_service.client.perform_query.assert_called_with('POST', '/jobs/create', data={'tasks': tasks}, headers=None, version='2.1')

    tasks = {'task_key': '123', 'notebook_task': {'notebook_path': '/test'}}
    tags = {"cost-center": "engineering","team": "jobs"}
    jobs_service.create_job(tasks=tasks, tags= tags, version='2.1')
    jobs_service.client.perform_query.assert_called_with('POST', '/jobs/create', data={'tasks': tasks, 'tags': tags}, headers=None, version='2.1')


@provide_conf
def test_create_dbt_task(jobs_service):
    git_source  = {
        'git_provider': 'github',
        'git_url': 'https://github.com/foo/bar',
        'git_branch': 'main'
    }

    tasks = [
      {
        'task_key': 'dbt',
        'dbt_task': {
            'commands': ['dbt test']
        }
      }
    ]

    jobs_service.create_job(git_source=git_source, tasks=tasks)
    jobs_service.client.perform_query.assert_called_with('POST', '/jobs/create', data={'git_source': git_source, 'tasks': tasks}, headers=None, version=None)


@provide_conf
def test_run_now_dbt_task(jobs_service):
    job_id = 1337
    dbt_commands = ['dbt test', 'dbt deps']

    jobs_service.run_now(job_id=job_id, dbt_commands=dbt_commands)
    jobs_service.client.perform_query.assert_called_with('POST', '/jobs/run-now', data={'job_id': job_id, 'dbt_commands': dbt_commands}, headers=None, version=None)


@provide_conf
def test_create_job_invalid_types(jobs_service):
    with pytest.raises(TypeError, match='new_cluster'):
        jobs_service.create_job(new_cluster=[])

    with pytest.raises(TypeError, match='email_notifications'):
        jobs_service.create_job(email_notifications=[])

    with pytest.raises(TypeError, match='schedule'):
        jobs_service.create_job(schedule=[])

    with pytest.raises(TypeError, match='git_source'):
        jobs_service.create_job(git_source=[])

    with pytest.raises(TypeError, match='notebook_task'):
        jobs_service.create_job(notebook_task=[])
        
    with pytest.raises(TypeError, match='spark_jar_task'):
        jobs_service.create_job(spark_jar_task=[])
        
    with pytest.raises(TypeError, match='spark_python_task'):
        jobs_service.create_job(spark_python_task=[])
        
    with pytest.raises(TypeError, match='spark_submit_task'):
        jobs_service.create_job(spark_submit_task=[])

    with pytest.raises(TypeError, match='dbt_task'):
        jobs_service.create_job(dbt_task=[])


@provide_conf
def test_update_job_invalid_types(jobs_service):
    with pytest.raises(TypeError, match='new_settings'):
        jobs_service.update_job(job_id=None, new_settings=[])



@provide_conf
def test_submit_run_invalid_types(jobs_service):
    with pytest.raises(TypeError, match='new_cluster'):
        jobs_service.submit_run(new_cluster=[])

    with pytest.raises(TypeError, match='email_notifications'):
        jobs_service.submit_run(email_notifications=[])

    with pytest.raises(TypeError, match='schedule'):
        jobs_service.submit_run(schedule=[])

    with pytest.raises(TypeError, match='git_source'):
        jobs_service.submit_run(git_source=[])

    with pytest.raises(TypeError, match='notebook_task'):
        jobs_service.submit_run(notebook_task=[])
        
    with pytest.raises(TypeError, match='spark_jar_task'):
        jobs_service.submit_run(spark_jar_task=[])
        
    with pytest.raises(TypeError, match='spark_python_task'):
        jobs_service.submit_run(spark_python_task=[])
        
    with pytest.raises(TypeError, match='spark_submit_task'):
        jobs_service.submit_run(spark_submit_task=[])

    with pytest.raises(TypeError, match='dbt_task'):
        jobs_service.submit_run(dbt_task=[])
