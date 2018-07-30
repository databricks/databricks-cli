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
# pylint:disable=too-many-locals
# pylint:disable=unused-argument

import os
import json
import mock
from requests.exceptions import HTTPError

import pytest

import databricks_cli.stack.api as api
from databricks_cli.stack.exceptions import StackError

TEST_STACK_PATH = 'stack/stack.json'
TEST_JOB_SETTINGS = {
    'name': 'test job'
}
TEST_RESOURCE_ID = 'test job'
TEST_JOB_RESOURCE = {
    api.RESOURCE_ID: TEST_RESOURCE_ID,
    api.RESOURCE_SERVICE: api.JOBS_SERVICE,
    api.RESOURCE_PROPERTIES: TEST_JOB_SETTINGS
}
TEST_JOB_PHYSICAL_ID = {'job_id': 1234}
TEST_WORKSPACE_NB_PROPERTIES = {
    'source_path': 'notebook.py',
    'path': '/test/notebook.py',
    'object_type': 'NOTEBOOK'
}
TEST_WORKSPACE_DIR_PROPERTIES = {
    'source_path': 'test/dir',
    'path': '/test/dir',
    'object_type': 'DIRECTORY'
}
TEST_DBFS_FILE_PROPERTIES = {
    'source_path': 'test.jar',
    'path': 'dbfs:/test/test.jar',
    'is_dir': False
}
TEST_DBFS_DIR_PROPERTIES = {
    'source_path': 'test/dir',
    'path': 'dbfs:/test/dir',
    'is_dir': True
}
TEST_DBFS_FILE_PHYSICAL_ID = {'path': 'dbfs:/test/test.jar'}
TEST_DBFS_DIR_PHYSICAL_ID = {'path': 'dbfs:/test/dir'}
TEST_RESOURCE_ID = 'test job'
TEST_RESOURCE_WORKSPACE_NB_ID = 'test notebook'
TEST_RESOURCE_WORKSPACE_DIR_ID = 'test directory'
TEST_WORKSPACE_NB_RESOURCE = {
    api.RESOURCE_ID: TEST_RESOURCE_WORKSPACE_NB_ID,
    api.RESOURCE_SERVICE: api.WORKSPACE_SERVICE,
    api.RESOURCE_PROPERTIES: TEST_WORKSPACE_NB_PROPERTIES
}
TEST_WORKSPACE_DIR_RESOURCE = {
    api.RESOURCE_ID: TEST_RESOURCE_WORKSPACE_DIR_ID,
    api.RESOURCE_SERVICE: api.WORKSPACE_SERVICE,
    api.RESOURCE_PROPERTIES: TEST_WORKSPACE_DIR_PROPERTIES
}
TEST_RESOURCE_DBFS_FILE_ID = 'test dbfs file'
TEST_RESOURCE_DBFS_DIR_ID = 'test dbfs directory'
TEST_DBFS_FILE_RESOURCE = {
    api.RESOURCE_ID: TEST_RESOURCE_DBFS_FILE_ID,
    api.RESOURCE_SERVICE: api.DBFS_SERVICE,
    api.RESOURCE_PROPERTIES: TEST_DBFS_FILE_PROPERTIES
}
TEST_DBFS_DIR_RESOURCE = {
    api.RESOURCE_ID: TEST_RESOURCE_DBFS_DIR_ID,
    api.RESOURCE_SERVICE: api.DBFS_SERVICE,
    api.RESOURCE_PROPERTIES: TEST_DBFS_DIR_PROPERTIES
}
TEST_JOB_STATUS = {
    api.RESOURCE_ID: TEST_RESOURCE_ID,
    api.RESOURCE_SERVICE: api.JOBS_SERVICE,
    api.RESOURCE_PHYSICAL_ID: TEST_JOB_PHYSICAL_ID
}
TEST_DBFS_FILE_STATUS = {
    api.RESOURCE_ID: TEST_RESOURCE_DBFS_FILE_ID,
    api.RESOURCE_SERVICE: api.DBFS_SERVICE,
    api.RESOURCE_PHYSICAL_ID: TEST_DBFS_FILE_PHYSICAL_ID
}
TEST_DBFS_DIR_STATUS = {
    api.RESOURCE_ID: TEST_RESOURCE_DBFS_DIR_ID,
    api.RESOURCE_SERVICE: api.DBFS_SERVICE,
    api.RESOURCE_PHYSICAL_ID: TEST_DBFS_DIR_PHYSICAL_ID
}
TEST_STACK = {
    api.STACK_NAME: "test-stack",
    api.STACK_RESOURCES: [TEST_JOB_RESOURCE,
                          TEST_WORKSPACE_NB_RESOURCE,
                          TEST_WORKSPACE_DIR_RESOURCE]
}
TEST_STATUS = {
    api.STACK_NAME: "test-stack",
    api.STACK_RESOURCES: [TEST_JOB_RESOURCE,
                          TEST_WORKSPACE_NB_RESOURCE,
                          TEST_WORKSPACE_DIR_RESOURCE,
                          TEST_DBFS_FILE_RESOURCE,
                          TEST_DBFS_DIR_RESOURCE],
    api.STACK_DEPLOYED: [TEST_JOB_STATUS,
                         TEST_DBFS_FILE_STATUS,
                         TEST_DBFS_DIR_STATUS]
}


class _TestJobsClient(object):
    def __init__(self):
        self.jobs_in_databricks = {}
        self.available_job_id = [1234, 12345]

    def get_job(self, job_id):
        if job_id not in self.jobs_in_databricks:
            # Job created is not found.
            raise HTTPError('Job not Found')
        else:
            return self.jobs_in_databricks[job_id]

    def reset_job(self, data):
        if data['job_id'] not in self.jobs_in_databricks:
            raise HTTPError('Job Not Found')
        self.jobs_in_databricks[data['job_id']]['job_settings'] = data['new_settings']

    def create_job(self, job_settings):
        job_id = self.available_job_id.pop()
        new_job_json = {'job_id': job_id,
                        'job_settings': job_settings.copy(),
                        'creator_user_name': 'testuser@example.com',
                        'created_time': 987654321}
        self.jobs_in_databricks[job_id] = new_job_json
        return new_job_json

    def _list_jobs_by_name(self, job_name):
        return [job for job in self.jobs_in_databricks.values()
                if job['job_settings']['name'] == job_name]


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
    def test_load_json_config(self, stack_api, tmpdir):
        """
            _load_json should read the same JSON content that was originally in the
            stack configuration JSON.
        """
        stack_path = os.path.join(tmpdir.strpath, TEST_STACK_PATH)
        os.makedirs(os.path.dirname(stack_path))
        with open(stack_path, "w+") as f:
            json.dump(TEST_STACK, f)
        config = stack_api._load_json(stack_path)
        assert config == TEST_STACK

    def test_generate_stack_status_path(self, stack_api, tmpdir):
        """
            The _generate_stack_status_path should add the word 'deployed' between the json file
            extension and the filename of the stack configuration file.
        """
        config_path = os.path.join(tmpdir.strpath, 'test.json')
        expected_status_path = os.path.join(tmpdir.strpath, 'test.deployed.json')
        generated_path = stack_api._generate_stack_status_path(config_path)
        assert expected_status_path == generated_path

        config_path = os.path.join(tmpdir.strpath, 'test.st-ack.json')
        expected_status_path = os.path.join(tmpdir.strpath, 'test.st-ack.deployed.json')
        generated_path = stack_api._generate_stack_status_path(config_path)
        assert expected_status_path == generated_path

    def test_save_load_stack_status(self, stack_api, tmpdir):
        """
            When saving the a stack status through _save_stack_status, it should be able to be
            loaded by _load_stack_status and have the same exact contents.
        """
        config_path = os.path.join(tmpdir.strpath, 'test.json')
        status_path = stack_api._generate_stack_status_path(config_path)
        stack_api._save_json(status_path, TEST_STATUS)

        status = stack_api._load_json(status_path)
        assert status == TEST_STATUS

    def test_deploy_relative_paths(self, stack_api, tmpdir):
        """
            When doing stack_api.deploy, in every call to stack_api._deploy_resource, the current
            working directory should be the same directory as where the stack config template is
            contained so that relative paths for resources can be relative to the stack config
            instead of where CLI calls the API functions.
        """
        config_working_dir = os.path.join(tmpdir.strpath, 'stack')
        config_path = os.path.join(config_working_dir, 'test.json')
        os.makedirs(config_working_dir)
        with open(config_path, 'w+') as f:
            json.dump(TEST_STACK, f)

        def _deploy_resource(resource, stack_status, **kwargs):
            assert os.getcwd() == config_working_dir
            return TEST_JOB_STATUS

        stack_api._deploy_resource = mock.Mock(wraps=_deploy_resource)
        stack_api.deploy(config_path)

    def test_deploy_job(self, stack_api):
        """
            stack_api._deploy_job should create a new job when 1) A physical_id is not given and
            a job with the same name does not exist in the settings.

            stack_api._deploy_job should reset/update an existing job when 1) A physical_id is given
            2) A physical_id is not given but one job with the same name exists.

            A StackError should be raised when 1) A physical_id is not given but there are multiple
            jobs with the same name that exist.
        """
        test_job_settings = TEST_JOB_SETTINGS
        alt_test_job_settings = {'name': 'alt test job'}  # Different name than TEST_JOB_SETTINGS
        stack_api.jobs_client = _TestJobsClient()
        # TEST CASE 1:
        # stack_api._deploy_job should create job if physical_id not given job doesn't exist
        res_physical_id_1, res_deploy_output_1 = stack_api._deploy_job(test_job_settings)
        assert stack_api.jobs_client.get_job(res_physical_id_1['job_id']) == res_deploy_output_1
        assert res_deploy_output_1['job_id'] == res_physical_id_1['job_id']
        assert test_job_settings == res_deploy_output_1['job_settings']

        # TEST CASE 2:
        # stack_api._deploy_job should reset job if physical_id given.
        res_physical_id_2, res_deploy_output_2 = stack_api._deploy_job(alt_test_job_settings,
                                                                       res_physical_id_1)
        # physical job id not changed from last update
        assert res_physical_id_2['job_id'] == res_physical_id_1['job_id']
        assert res_deploy_output_2['job_id'] == res_physical_id_2['job_id']
        assert alt_test_job_settings == res_deploy_output_2['job_settings']

        # TEST CASE 3:
        # stack_api._deploy_job should reset job if a physical_id not given, but job with same name
        # found
        alt_test_job_settings['new_property'] = 'new_property_value'
        res_physical_id_3, res_deploy_output_3 = stack_api._deploy_job(alt_test_job_settings)
        # physical job id not changed from last update
        assert res_physical_id_3['job_id'] == res_physical_id_2['job_id']
        assert res_deploy_output_3['job_id'] == res_physical_id_3['job_id']
        assert alt_test_job_settings == res_deploy_output_3['job_settings']

        # TEST CASE 4
        # If a physical_id is not given but there is already multiple jobs of the same name in
        # databricks, an error should be raised
        # Add new job with different physical id but same name settings as alt_test_job_settings
        stack_api.jobs_client.jobs_in_databricks[123] = {
            'job_id': 123,
            'job_settings': alt_test_job_settings
        }
        with pytest.raises(StackError):
            stack_api._deploy_job(alt_test_job_settings)

    def test_deploy_workspace(self, stack_api, tmpdir):
        """
            stack_api._deploy_workspace should call certain workspace client functions depending
            on object_type and error when object_type is defined incorrectly.
        """
        test_deploy_output = {'test': 'test'}  # default deploy_output return value

        stack_api.workspace_client.client = mock.MagicMock()
        stack_api.workspace_client.client.get_status.return_value = test_deploy_output
        stack_api.workspace_client.import_workspace = mock.MagicMock()
        stack_api.workspace_client.import_workspace_dir = mock.MagicMock()

        test_workspace_nb_properties = TEST_WORKSPACE_NB_PROPERTIES.copy()
        test_workspace_nb_properties.update(
            {'source_path': os.path.join(tmpdir.strpath,
                                         test_workspace_nb_properties['source_path'])})
        with open(test_workspace_nb_properties['source_path'], 'w') as f:
            f.write("print('test')\n")
        test_workspace_dir_properties = TEST_WORKSPACE_DIR_PROPERTIES.copy()
        test_workspace_dir_properties.update(
            {'source_path': os.path.join(tmpdir.strpath,
                                         test_workspace_dir_properties['source_path'])})
        os.makedirs(test_workspace_dir_properties['source_path'])

        dir_physical_id, dir_deploy_output = \
            stack_api._deploy_workspace(test_workspace_dir_properties, None, True)
        stack_api.workspace_client.import_workspace_dir.assert_called_once()
        assert stack_api.workspace_client.import_workspace_dir.call_args[0][0] == \
            test_workspace_dir_properties['source_path']
        assert stack_api.workspace_client.import_workspace_dir.call_args[0][1] == \
            test_workspace_dir_properties['path']
        assert dir_physical_id == {'path': test_workspace_dir_properties['path']}
        assert dir_deploy_output == test_deploy_output

        nb_physical_id, nb_deploy_output = \
            stack_api._deploy_workspace(test_workspace_nb_properties, None, True)
        stack_api.workspace_client.import_workspace.assert_called_once()
        assert stack_api.workspace_client.import_workspace.call_args[0][0] == \
            test_workspace_nb_properties['source_path']
        assert stack_api.workspace_client.import_workspace.call_args[0][1] == \
            test_workspace_nb_properties['path']
        assert nb_physical_id == {'path': test_workspace_nb_properties['path']}
        assert nb_deploy_output == test_deploy_output

        # Should raise error if resource object_type doesn't match actually is in filesystem.
        test_workspace_dir_properties.update({'object_type': 'NOTEBOOK'})
        with pytest.raises(StackError):
            stack_api._deploy_workspace(test_workspace_dir_properties, None, True)

        # Should raise error if object_type is not NOTEBOOK or DIRECTORY
        test_workspace_dir_properties.update({'object_type': 'INVALID_TYPE'})
        with pytest.raises(StackError):
            stack_api._deploy_workspace(test_workspace_dir_properties, None, True)

    def test_deploy_dbfs(self, stack_api, tmpdir):
        """
            stack_api._deploy_dbfs should call certain dbfs client functions depending
            on object_type and error when object_type is defined incorrectly.
        """
        test_deploy_output = {'test': 'test'}  # default deploy_output return value

        stack_api.dbfs_client.client = mock.MagicMock()
        stack_api.dbfs_client.client.get_status.return_value = test_deploy_output
        stack_api.dbfs_client.cp = mock.MagicMock()

        test_dbfs_file_properties = TEST_DBFS_FILE_PROPERTIES.copy()
        test_dbfs_file_properties.update(
            {'source_path': os.path.join(tmpdir.strpath,
                                         test_dbfs_file_properties['source_path'])})
        with open(test_dbfs_file_properties['source_path'], 'w') as f:
            f.write("print('test')\n")
        test_dbfs_dir_properties = TEST_DBFS_DIR_PROPERTIES.copy()
        test_dbfs_dir_properties.update(
            {'source_path': os.path.join(tmpdir.strpath,
                                         test_dbfs_dir_properties['source_path'])})
        os.makedirs(test_dbfs_dir_properties['source_path'])

        dir_physical_id, dir_deploy_output = \
            stack_api._deploy_dbfs(test_dbfs_dir_properties, None, True)
        assert stack_api.dbfs_client.cp.call_count == 1
        assert stack_api.dbfs_client.cp.call_args[1]['recursive'] is True
        assert stack_api.dbfs_client.cp.call_args[1]['overwrite'] is True
        assert stack_api.dbfs_client.cp.call_args[1]['src'] == \
            test_dbfs_dir_properties['source_path']
        assert stack_api.dbfs_client.cp.call_args[1]['dst'] == \
            test_dbfs_dir_properties['path']
        assert dir_physical_id == {'path': test_dbfs_dir_properties['path']}
        assert dir_deploy_output == test_deploy_output

        nb_physical_id, nb_deploy_output = \
            stack_api._deploy_dbfs(test_dbfs_file_properties, None, True)
        assert stack_api.dbfs_client.cp.call_count == 2
        assert stack_api.dbfs_client.cp.call_args[1]['recursive'] is False
        assert stack_api.dbfs_client.cp.call_args[1]['overwrite'] is True
        assert stack_api.dbfs_client.cp.call_args[1]['src'] == \
            test_dbfs_file_properties['source_path']
        assert stack_api.dbfs_client.cp.call_args[1]['dst'] == \
            test_dbfs_file_properties['path']
        assert nb_physical_id == {'path': test_dbfs_file_properties['path']}
        assert nb_deploy_output == test_deploy_output

        # Should raise error if resource properties is_dir field isn't consistent with whether the
        # resource is a directory or not locally.
        test_dbfs_dir_properties.update({'is_dir': False})
        with pytest.raises(StackError):
            stack_api._deploy_dbfs(test_dbfs_dir_properties, None, True)

    def test_deploy_resource(self, stack_api):
        """
           stack_api._deploy_resource should return relevant fields in output if deploy done
           correctly.
        """
        # TODO(alinxie) Change this test to directly call stack_api.deploy/stack_api.deploy_config
        # A job resource should have _deploy_resource call on _deploy_job
        stack_api._deploy_job = mock.MagicMock()
        test_job_physical_id = {'job_id': 12345}
        stack_api._deploy_job.return_value = (test_job_physical_id, {})
        test_job_resource_status = {api.RESOURCE_PHYSICAL_ID: test_job_physical_id}
        new_resource_status = stack_api._deploy_resource(TEST_JOB_RESOURCE,
                                                         resource_status=test_job_resource_status)
        assert api.RESOURCE_ID in new_resource_status
        assert api.RESOURCE_PHYSICAL_ID in new_resource_status
        assert api.RESOURCE_DEPLOY_OUTPUT in new_resource_status
        assert api.RESOURCE_SERVICE in new_resource_status
        stack_api._deploy_job.assert_called()
        assert stack_api._deploy_job.call_args[0][0] == TEST_JOB_RESOURCE[api.RESOURCE_PROPERTIES]
        assert stack_api._deploy_job.call_args[0][1] == test_job_physical_id

        # A workspace resource should have _deploy_resource call on _deploy_workspace
        stack_api._deploy_workspace = mock.MagicMock()
        test_workspace_physical_id = {'path': '/test/path'}
        stack_api._deploy_workspace.return_value = (test_workspace_physical_id, {})
        test_workspace_resource_status = {api.RESOURCE_PHYSICAL_ID: test_workspace_physical_id}
        stack_api._deploy_resource(TEST_WORKSPACE_NB_RESOURCE,
                                   resource_status=test_workspace_resource_status,
                                   overwrite_notebook=True)
        stack_api._deploy_workspace.assert_called()
        assert stack_api._deploy_workspace.call_args[0][0] == \
            TEST_WORKSPACE_NB_RESOURCE[api.RESOURCE_PROPERTIES]
        assert stack_api._deploy_workspace.call_args[0][1] == test_workspace_physical_id

        # A dbfs resource should have _deploy_resource call on _deploy_workspace
        stack_api._deploy_dbfs = mock.MagicMock()
        stack_api._deploy_dbfs.return_value = (TEST_DBFS_FILE_PHYSICAL_ID, {})
        stack_api._deploy_resource(TEST_DBFS_FILE_RESOURCE,
                                   resource_status=TEST_DBFS_FILE_STATUS,
                                   overwrite_dbfs=True)
        stack_api._deploy_dbfs.assert_called()
        assert stack_api._deploy_dbfs.call_args[0][0] == \
            TEST_DBFS_FILE_RESOURCE[api.RESOURCE_PROPERTIES]
        assert stack_api._deploy_dbfs.call_args[0][1] == \
            TEST_DBFS_FILE_STATUS[api.RESOURCE_PHYSICAL_ID]

        # If there is a nonexistent type, raise a StackError.
        resource_badtype = {
            api.RESOURCE_SERVICE: 'nonexist',
            api.RESOURCE_ID: 'test',
            api.RESOURCE_PROPERTIES: {'test': 'test'}
        }
        with pytest.raises(StackError):
            stack_api._deploy_resource(resource_badtype)
