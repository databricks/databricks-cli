# Databricks CLI
# Copyright 2018 Databricks, Inc.
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

import os
import json
import mock
from requests.exceptions import HTTPError

import pytest

import databricks_cli.stack.api as api
from databricks_cli.stack.exceptions import StackError

TEST_STACK_PATH = 'stack/stack.json'
TEST_JOB_SETTINGS = {
    'name': 'my test job'
}
TEST_JOB_ALT_SETTINGS = {
    'name': 'my test job'
}
TEST_JOB_RESOURCE = {
    api.RESOURCE_ID: "job 1",
    api.RESOURCE_SERVICE: api.JOBS_SERVICE,
    api.RESOURCE_PROPERTIES: TEST_JOB_SETTINGS
}
TEST_STACK = {
    api.STACK_NAME: "test-stack",
    api.STACK_RESOURCES: [TEST_JOB_RESOURCE]
}
TEST_STATUS = {
    api.STACK_NAME: "test-stack",
    api.STACK_RESOURCES: [TEST_JOB_RESOURCE],
    api.STACK_DEPLOYED: []
}


@pytest.fixture()
def stack_api():
    workspace_api_mock = mock.patch('databricks_cli.stack.api.WorkspaceApi')
    jobs_api_mock = mock.patch('databricks_cli.stack.api.JobsApi')
    dbfs_api_mock = mock.patch('databricks_cli.stack.api.DbfsApi')
    workspace_api_mock.return_value = mock.MagicMock()
    jobs_api_mock.return_value = mock.MagicMock()
    dbfs_api_mock.return_value = mock.MagicMock()
    stack_api = api.StackApi(mock.MagicMock())
    yield stack_api


class TestStackApi(object):
    def test_read_config(self, stack_api, tmpdir):
        """
            Test reading a stack configuration template
        """
        stack_path = os.path.join(tmpdir.strpath, TEST_STACK_PATH)
        os.makedirs(os.path.dirname(stack_path))
        with open(stack_path, "w+") as f:
            json.dump(TEST_STACK, f)
        config = stack_api._parse_config_file(stack_path)
        assert config == TEST_STACK

    def test_read_status(self, stack_api, tmpdir):
        """
            Test reading and parsing a deployed stack's status JSON file.
        """
        config_path = os.path.join(tmpdir.strpath, 'test.json')
        status_path = os.path.join(tmpdir.strpath, 'test.deployed.json')
        with open(config_path, "w+") as f:
            json.dump(TEST_STACK, f)
        with open(status_path, "w+") as f:
            json.dump(TEST_STATUS, f)
        status = stack_api._load_deploy_metadata(stack_path=config_path)
        assert status == TEST_STATUS
        assert stack_api.deployed_resource_config == TEST_STATUS[api.STACK_RESOURCES]
        assert all(resource[api.RESOURCE_ID] in stack_api.deployed_resources
                   for resource in TEST_STATUS[api.STACK_DEPLOYED])

    def test_default_status_path(self, stack_api, tmpdir):
        config_path = os.path.join(tmpdir.strpath, 'test.json')
        expected_status_path = os.path.join(tmpdir.strpath, 'test.deployed.json')
        generated_path = stack_api._generate_stack_status_path(config_path)
        assert expected_status_path == generated_path

        config_path = os.path.join(tmpdir.strpath, 'test.st-ack.json')
        expected_status_path = os.path.join(tmpdir.strpath, 'test.st-ack.deployed.json')
        generated_path = stack_api._generate_stack_status_path(config_path)
        assert expected_status_path == generated_path

    def test_store_status(self, stack_api, tmpdir):
        config_path = os.path.join(tmpdir.strpath, 'test.json')
        default_path = os.path.join(tmpdir.strpath, 'test.deployed.json')
        test_data = {'test': 'test'}
        stack_api._store_deploy_metadata(config_path, test_data)

        status = stack_api._load_deploy_metadata(config_path)
        assert status == test_data
        assert os.path.exists(default_path)

    def test_relative_paths(self, stack_api, tmpdir):
        """
            Test that the current working directory when deploying or downloading resource is same
            as where the config path lies.
        """
        config_working_dir = os.path.join(tmpdir.strpath, 'stack')
        config_path = os.path.join(config_working_dir, 'test.json')
        os.makedirs(config_working_dir)
        with open(config_path, 'w+') as f:
            json.dump(TEST_STACK, f)
        initial_cwd = os.getcwd()

        def _deploy_resource(resource):
            assert resource is not None  # just to pass lint
            assert os.getcwd() == config_working_dir
            return {}

        stack_api.deploy_resource = mock.Mock(wraps=_deploy_resource)
        stack_api.deploy(config_path)
        assert os.getcwd() == initial_cwd  # Make sure current working directory didn't change

    def deploy_error_path(self, stack_api, tmpdir):
        """
            Test that an error in deployment doesn't change current working directory.
        """
        config_working_dir = os.path.join(tmpdir.strpath, 'stack')
        initial_cwd = os.getcwd()
        config_path = os.path.join(config_working_dir, 'test.json')
        os.makedirs(config_working_dir)
        with open(config_path, 'w+') as f:
            json.dump({'name': 'test'}, f)
        # No 'resources' key will cause a key error
        try:
            stack_api.deploy(config_path)
        except KeyError:
            pass
        assert os.getcwd() == initial_cwd

        try:
            stack_api.download(config_path)
        except KeyError:
            pass
        assert os.getcwd() == initial_cwd

    def test_deploy_job(self, stack_api):
        """
            Test Deploy Job Functionality
        """
        job_physical_id = 12345
        job_deploy_output = {'job_id': job_physical_id, 'job_settings': TEST_JOB_SETTINGS}

        def _get_job(job_id):
            if job_id != job_physical_id:
                raise HTTPError()
            else:
                return job_deploy_output

        def _reset_job(data):
            if data['job_id'] != job_deploy_output['job_id']:
                raise Exception('Job Not Found')
            job_deploy_output['job_settings'] = data['new_settings']

        def _create_job(job_settings):
            job_deploy_output['job_settings'] = job_settings
            return {'job_id': job_physical_id}

        stack_api.jobs_client.create_job = mock.Mock(wraps=_create_job)
        stack_api.jobs_client.get_job = mock.Mock(wraps=_get_job)
        stack_api.jobs_client.reset_job = mock.Mock(wraps=_reset_job)

        # Deploy New job
        res_physical_id, res_deploy_output = stack_api.deploy_job('test job', TEST_JOB_SETTINGS)

        assert 'job_id' in res_physical_id
        assert res_physical_id['job_id'] == job_physical_id
        assert res_deploy_output == job_deploy_output

        # Updating job
        job_deploy_output['job_settings'] = TEST_JOB_ALT_SETTINGS
        res_physical_id, res_deploy_output = stack_api.deploy_job('test job', TEST_JOB_SETTINGS,
                                                                  res_physical_id)
        assert res_deploy_output == job_deploy_output
        assert 'job_id' in res_physical_id
        assert res_physical_id['job_id'] == job_physical_id

        # Try to update job that doesn't exist anymore
        job_physical_id = 123456
        job_deploy_output = {'job_id': job_physical_id, 'job_settings': TEST_JOB_SETTINGS}
        res_physical_id, res_deploy_output = stack_api.deploy_job('test job', TEST_JOB_SETTINGS,
                                                                  res_physical_id)
        assert res_deploy_output == job_deploy_output
        assert 'job_id' in res_physical_id
        assert res_physical_id['job_id'] == job_physical_id

        assert stack_api.jobs_client.get_job.call_count == 5
        assert stack_api.jobs_client.reset_job.call_count == 1
        assert stack_api.jobs_client.create_job.call_count == 2

    def test_deploy_resource(self, stack_api):
        stack_api.jobs_client.deploy_job = mock.MagicMock()
        stack_api.jobs_client.deploy_job.return_value = (12345, {'job_id': 12345})

        deploy_info = stack_api.deploy_resource(TEST_JOB_RESOURCE)
        assert api.RESOURCE_ID in deploy_info
        assert api.RESOURCE_PHYSICAL_ID in deploy_info
        assert api.RESOURCE_DEPLOY_OUTPUT in deploy_info
        assert api.RESOURCE_SERVICE in deploy_info

        # If there is a nonexistent type, just return None and continue on with deployment
        resource_badtype = {
            api.RESOURCE_SERVICE: 'nonexist',
            api.RESOURCE_ID: 'test',
            api.RESOURCE_PROPERTIES: {'test': 'test'}
        }
        with pytest.raises(StackError):
            stack_api.deploy_resource(resource_badtype)

        # Missing a key, raise config error
        d = TEST_JOB_RESOURCE.copy()
        with pytest.raises(StackError):
            d.pop(api.RESOURCE_SERVICE)
            stack_api.deploy_resource(d)
        d = TEST_JOB_RESOURCE.copy()
        with pytest.raises(StackError):
            d.pop(api.RESOURCE_ID)
            stack_api.deploy_resource(d)
        d = TEST_JOB_RESOURCE.copy()
        with pytest.raises(StackError):
            d.pop(api.RESOURCE_PROPERTIES)
            stack_api.deploy_resource(d)
