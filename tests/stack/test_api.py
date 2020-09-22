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
import copy
import mock
from requests.exceptions import HTTPError

import pytest

import databricks_cli.stack.api as api
import databricks_cli.workspace.api as workspace_api
from databricks_cli.stack.exceptions import StackError
from databricks_cli.version import version as CLI_VERSION

TEST_JOB_SETTINGS = {
    api.JOBS_RESOURCE_NAME: 'test job'
}
TEST_JOB_NONEXISTING_SETTINGS = {
    api.JOBS_RESOURCE_NAME: 'non-existing test job in workspace'
}
TEST_JOB_RESOURCE_ID = 'test job'
TEST_JOB_RESOURCE = {
    api.RESOURCE_ID: TEST_JOB_RESOURCE_ID,
    api.RESOURCE_SERVICE: api.JOBS_SERVICE,
    api.RESOURCE_PROPERTIES: TEST_JOB_SETTINGS
}
TEST_JOB_DATABRICKS_ID = {api.JOBS_RESOURCE_JOB_ID: 1234}
TEST_WORKSPACE_NB_PROPERTIES = {
    api.WORKSPACE_RESOURCE_SOURCE_PATH: 'test/notebook.py',
    api.WORKSPACE_RESOURCE_PATH: '/test/notebook.py',
    api.WORKSPACE_RESOURCE_OBJECT_TYPE: workspace_api.NOTEBOOK
}
TEST_WORKSPACE_DIR_PROPERTIES = {
    api.WORKSPACE_RESOURCE_SOURCE_PATH: 'test/workspace/dir',
    api.WORKSPACE_RESOURCE_PATH: '/test/dir',
    api.WORKSPACE_RESOURCE_OBJECT_TYPE: workspace_api.DIRECTORY
}
TEST_WORKSPACE_NB_DATABRICKS_ID = {api.WORKSPACE_RESOURCE_PATH: '/test/notebook.py'}
TEST_WORKSPACE_DIR_DATABRICKS_ID = {api.WORKSPACE_RESOURCE_PATH: '/test/dir'}
TEST_DBFS_FILE_PROPERTIES = {
    api.DBFS_RESOURCE_SOURCE_PATH: 'test.jar',
    api.DBFS_RESOURCE_PATH: 'dbfs:/test/test.jar',
    api.DBFS_RESOURCE_IS_DIR: False
}
TEST_DBFS_DIR_PROPERTIES = {
    api.DBFS_RESOURCE_SOURCE_PATH: 'test/dbfs/dir',
    api.DBFS_RESOURCE_PATH: 'dbfs:/test/dir',
    api.DBFS_RESOURCE_IS_DIR: True
}
TEST_DBFS_FILE_DATABRICKS_ID = {api.DBFS_RESOURCE_PATH: 'dbfs:/test/test.jar'}
TEST_DBFS_DIR_DATABRICKS_ID = {api.DBFS_RESOURCE_PATH: 'dbfs:/test/dir'}
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
    api.RESOURCE_DATABRICKS_ID: TEST_JOB_DATABRICKS_ID
}
TEST_WORKSPACE_NB_STATUS = {
    api.RESOURCE_ID: TEST_RESOURCE_WORKSPACE_NB_ID,
    api.RESOURCE_SERVICE: api.WORKSPACE_SERVICE,
    api.RESOURCE_DATABRICKS_ID: TEST_WORKSPACE_NB_DATABRICKS_ID
}
TEST_WORKSPACE_DIR_STATUS = {
    api.RESOURCE_ID: TEST_RESOURCE_WORKSPACE_DIR_ID,
    api.RESOURCE_SERVICE: api.WORKSPACE_SERVICE,
    api.RESOURCE_DATABRICKS_ID: TEST_WORKSPACE_DIR_DATABRICKS_ID
}
TEST_DBFS_FILE_STATUS = {
    api.RESOURCE_ID: TEST_RESOURCE_DBFS_FILE_ID,
    api.RESOURCE_SERVICE: api.DBFS_SERVICE,
    api.RESOURCE_DATABRICKS_ID: TEST_DBFS_FILE_DATABRICKS_ID
}
TEST_DBFS_DIR_STATUS = {
    api.RESOURCE_ID: TEST_RESOURCE_DBFS_DIR_ID,
    api.RESOURCE_SERVICE: api.DBFS_SERVICE,
    api.RESOURCE_DATABRICKS_ID: TEST_DBFS_DIR_DATABRICKS_ID
}
TEST_STACK = {
    api.STACK_NAME: "test-stack",
    api.STACK_RESOURCES: [
        TEST_JOB_RESOURCE,
        TEST_WORKSPACE_NB_RESOURCE,
        TEST_WORKSPACE_DIR_RESOURCE,
        TEST_DBFS_FILE_RESOURCE,
        TEST_DBFS_DIR_RESOURCE,
        {
            api.RESOURCE_ID: "NoStatusResource",
            api.RESOURCE_SERVICE: api.DBFS_SERVICE,
            api.RESOURCE_WRITE_STATUS: False,
            api.RESOURCE_PROPERTIES:  {
                api.DBFS_RESOURCE_SOURCE_PATH: 'test.jar',
                api.DBFS_RESOURCE_PATH: 'dbfs:/test/test-no-status.jar',
                api.DBFS_RESOURCE_IS_DIR: False
            }
        }
    ]
}
TEST_STATUS = {
    api.STACK_NAME: "test-stack",
    api.CLI_VERSION_KEY: CLI_VERSION,
    api.STACK_DEPLOYED: [TEST_JOB_STATUS,
                         TEST_WORKSPACE_NB_STATUS,
                         TEST_WORKSPACE_DIR_STATUS,
                         TEST_DBFS_FILE_STATUS,
                         TEST_DBFS_DIR_STATUS,
                         ]
}


class _TestJobsClient(object):
    def __init__(self):
        self.jobs_in_databricks = {}
        self.available_job_id = [1234, 12345]
        self.nonexisting_job_id = 111

    def get_job(self, job_id, headers=None):
        if job_id not in self.jobs_in_databricks:
            # Job created is not found.
            raise HTTPError('Job not Found')
        return self.jobs_in_databricks[job_id]

    def reset_job(self, data, headers=None):
        if data[api.JOBS_RESOURCE_JOB_ID] not in self.jobs_in_databricks:
            raise HTTPError('Job Not Found')
        self.jobs_in_databricks[data[api.JOBS_RESOURCE_JOB_ID]]['job_settings'] = \
            data['new_settings']

    def create_job(self, job_settings, headers=None):
        job_id = self.available_job_id.pop()
        new_job_json = {api.JOBS_RESOURCE_JOB_ID: job_id,
                        'job_settings': job_settings.copy(),
                        'creator_user_name': 'testuser@example.com',
                        'created_time': 987654321}
        self.jobs_in_databricks[job_id] = new_job_json
        return new_job_json

    def _list_jobs_by_name(self, job_name, headers=None):
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
    def test_deploy_job(self, stack_api):
        """
            stack_api._deploy_job should create a new job when 1) A databricks_id is not given and
            a job with the same name does not exist in the settings.

            stack_api._deploy_job should reset/update an existing job when 1) A databricks_id is
            given
            2) A databricks_id is not given but one job with the same name exists.

            A StackError should be raised when 1) A databricks_id is not given but there are
            multiple jobs with the same name that exist.
        """
        test_job_settings = TEST_JOB_SETTINGS
        # Different name than TEST_JOB_SETTINGS
        alt_test_job_settings = {api.JOBS_RESOURCE_NAME: 'alt test job'}
        stack_api.jobs_client = _TestJobsClient()
        # TEST CASE 1:
        # stack_api._deploy_job should create job if databricks_id not given job doesn't exist
        res_databricks_id_1 = stack_api._deploy_job(test_job_settings)
        assert res_databricks_id_1 == {api.JOBS_RESOURCE_JOB_ID: 12345}

        # TEST CASE 2:
        # stack_api._deploy_job should reset job if databricks_id given.
        res_databricks_id_2 = stack_api._deploy_job(alt_test_job_settings,
                                                    res_databricks_id_1)
        # physical job id not changed from last update
        assert res_databricks_id_2[api.JOBS_RESOURCE_JOB_ID] == \
            res_databricks_id_1[api.JOBS_RESOURCE_JOB_ID]

        # TEST CASE 3:
        # stack_api._deploy_job should reset job if a databricks_id not given, but job with same
        # name found
        alt_test_job_settings['new_property'] = 'new_property_value'
        res_databricks_id_3 = stack_api._deploy_job(alt_test_job_settings)
        # physical job id not changed from last update
        assert res_databricks_id_3[api.JOBS_RESOURCE_JOB_ID] == \
            res_databricks_id_2[api.JOBS_RESOURCE_JOB_ID]

        # TEST CASE 4
        # If a databricks_id is not given but there is already multiple jobs of the same name in
        # databricks, an error should be raised
        # Add new job with different physical id but same name settings as alt_test_job_settings
        stack_api.jobs_client.jobs_in_databricks[123] = {
            api.JOBS_RESOURCE_JOB_ID: 123,
            'job_settings': alt_test_job_settings
        }
        with pytest.raises(StackError):
            stack_api._deploy_job(alt_test_job_settings)

        # TEST CASE 5
        # If a databricks_id is not found in workspace, then abort
        nonexisting_job_settings = TEST_JOB_NONEXISTING_SETTINGS
        nonexisting_databricks_id = {
            api.JOBS_RESOURCE_JOB_ID: stack_api.jobs_client.nonexisting_job_id
        }
        # Job deployment is aborted. Error message about the inconsistency should appear
        with pytest.raises(StackError):
            stack_api._deploy_job(nonexisting_job_settings, nonexisting_databricks_id)

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
            {api.WORKSPACE_RESOURCE_SOURCE_PATH: os.path.join(tmpdir.strpath,
                                                              test_workspace_nb_properties[
                                                                  api.WORKSPACE_RESOURCE_SOURCE_PATH
                                                              ])})
        os.makedirs(
            os.path.dirname(test_workspace_nb_properties[api.WORKSPACE_RESOURCE_SOURCE_PATH]))
        with open(test_workspace_nb_properties[api.WORKSPACE_RESOURCE_SOURCE_PATH], 'w') as f:
            f.write("print('test')\n")
        test_workspace_dir_properties = TEST_WORKSPACE_DIR_PROPERTIES.copy()
        test_workspace_dir_properties.update(
            {api.WORKSPACE_RESOURCE_SOURCE_PATH: os.path.join(tmpdir.strpath,
                                                              test_workspace_dir_properties[
                                                                  api.WORKSPACE_RESOURCE_SOURCE_PATH
                                                              ])})
        os.makedirs(test_workspace_dir_properties[api.WORKSPACE_RESOURCE_SOURCE_PATH])

        # Test Input of Workspace directory properties.
        dir_databricks_id = \
            stack_api._deploy_workspace(test_workspace_dir_properties, None, True)
        stack_api.workspace_client.import_workspace_dir.assert_called_once()
        assert stack_api.workspace_client.import_workspace_dir.call_args[0][0] == \
            test_workspace_dir_properties[api.WORKSPACE_RESOURCE_SOURCE_PATH]
        assert stack_api.workspace_client.import_workspace_dir.call_args[0][1] == \
            test_workspace_dir_properties[api.WORKSPACE_RESOURCE_PATH]
        assert dir_databricks_id == {
            api.WORKSPACE_RESOURCE_PATH: test_workspace_dir_properties[api.WORKSPACE_RESOURCE_PATH]}

        # Test Input of Workspace notebook properties.
        nb_databricks_id = \
            stack_api._deploy_workspace(test_workspace_nb_properties, None, True)
        stack_api.workspace_client.import_workspace.assert_called_once()
        assert stack_api.workspace_client.import_workspace.call_args[0][0] == \
            test_workspace_nb_properties[api.WORKSPACE_RESOURCE_SOURCE_PATH]
        assert stack_api.workspace_client.import_workspace.call_args[0][1] == \
            test_workspace_nb_properties[api.WORKSPACE_RESOURCE_PATH]
        assert nb_databricks_id == {api.WORKSPACE_RESOURCE_PATH:
                                    test_workspace_nb_properties[api.WORKSPACE_RESOURCE_PATH]}

        # Test Input of Workspace notebook with html source
        test_workspace_nb_properties.update(
            {api.WORKSPACE_RESOURCE_SOURCE_PATH: 'test/notebook.html'})
        nb_databricks_id = \
            stack_api._deploy_workspace(test_workspace_nb_properties, None, True)
        assert stack_api.workspace_client.import_workspace.call_args[0][0] == 'test/notebook.html'

        # Test Input of Workspace notebook with dbc source
        test_workspace_nb_properties.update(
            {api.WORKSPACE_RESOURCE_SOURCE_PATH: 'test/notebook.dbc'})
        nb_databricks_id = \
            stack_api._deploy_workspace(test_workspace_nb_properties, None, True)
        assert stack_api.workspace_client.import_workspace.call_args[0][0] == 'test/notebook.dbc'

        # Should raise error if resource object_type doesn't match actually is in filesystem.
        test_workspace_dir_properties.update(
            {api.WORKSPACE_RESOURCE_OBJECT_TYPE: workspace_api.NOTEBOOK})
        with pytest.raises(StackError):
            stack_api._deploy_workspace(test_workspace_dir_properties, None, True)

        # Should raise error if object_type is not NOTEBOOK or DIRECTORY
        test_workspace_dir_properties.update({api.WORKSPACE_RESOURCE_OBJECT_TYPE: 'INVALID_TYPE'})
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
            {api.DBFS_RESOURCE_SOURCE_PATH: os.path.join(tmpdir.strpath,
                                                         test_dbfs_file_properties[
                                                             api.DBFS_RESOURCE_SOURCE_PATH])})
        with open(test_dbfs_file_properties[api.DBFS_RESOURCE_SOURCE_PATH], 'w') as f:
            f.write("print('test')\n")
        test_dbfs_dir_properties = TEST_DBFS_DIR_PROPERTIES.copy()
        test_dbfs_dir_properties.update(
            {api.DBFS_RESOURCE_SOURCE_PATH: os.path.join(tmpdir.strpath,
                                                         test_dbfs_dir_properties[
                                                             api.DBFS_RESOURCE_SOURCE_PATH])})
        os.makedirs(test_dbfs_dir_properties[api.DBFS_RESOURCE_SOURCE_PATH])

        dir_databricks_id = \
            stack_api._deploy_dbfs(test_dbfs_dir_properties, None, True)
        assert stack_api.dbfs_client.cp.call_count == 1
        assert stack_api.dbfs_client.cp.call_args[1]['recursive'] is True
        assert stack_api.dbfs_client.cp.call_args[1]['overwrite'] is True
        assert stack_api.dbfs_client.cp.call_args[1]['src'] == \
            test_dbfs_dir_properties[api.DBFS_RESOURCE_SOURCE_PATH]
        assert stack_api.dbfs_client.cp.call_args[1]['dst'] == \
            test_dbfs_dir_properties[api.DBFS_RESOURCE_PATH]
        assert dir_databricks_id == {api.DBFS_RESOURCE_PATH:
                                     test_dbfs_dir_properties[api.DBFS_RESOURCE_PATH]}

        nb_databricks_id = \
            stack_api._deploy_dbfs(test_dbfs_file_properties, None, True)
        assert stack_api.dbfs_client.cp.call_count == 2
        assert stack_api.dbfs_client.cp.call_args[1]['recursive'] is False
        assert stack_api.dbfs_client.cp.call_args[1]['overwrite'] is True
        assert stack_api.dbfs_client.cp.call_args[1]['src'] == \
            test_dbfs_file_properties[api.DBFS_RESOURCE_SOURCE_PATH]
        assert stack_api.dbfs_client.cp.call_args[1]['dst'] == \
            test_dbfs_file_properties[api.DBFS_RESOURCE_PATH]
        assert nb_databricks_id == {api.DBFS_RESOURCE_PATH:
                                    test_dbfs_file_properties[api.DBFS_RESOURCE_PATH]}

        # Should raise error if resource properties is_dir field isn't consistent with whether the
        # resource is a directory or not locally.
        test_dbfs_dir_properties.update({api.DBFS_RESOURCE_IS_DIR: False})
        with pytest.raises(StackError):
            stack_api._deploy_dbfs(test_dbfs_dir_properties, None, True)

    def test_deploy_resource(self, stack_api):
        """
           stack_api._deploy_resource should return relevant fields in output if deploy done
           correctly.
        """
        # TODO(alinxie) Change this test to directly call stack_api.deploy
        # A job resource should have _deploy_resource call on _deploy_job
        stack_api._deploy_job = mock.MagicMock()
        test_job_databricks_id = {api.JOBS_RESOURCE_JOB_ID: 12345}
        stack_api._deploy_job.return_value = (test_job_databricks_id, {})
        test_job_resource_status = {api.RESOURCE_DATABRICKS_ID: test_job_databricks_id}
        new_resource_status = stack_api._deploy_resource(TEST_JOB_RESOURCE,
                                                         resource_status=test_job_resource_status)
        assert api.RESOURCE_ID in new_resource_status
        assert api.RESOURCE_DATABRICKS_ID in new_resource_status
        assert api.RESOURCE_SERVICE in new_resource_status
        stack_api._deploy_job.assert_called()
        assert stack_api._deploy_job.call_args[0][0] == TEST_JOB_RESOURCE[api.RESOURCE_PROPERTIES]
        assert stack_api._deploy_job.call_args[0][1] == test_job_databricks_id

        # A workspace resource should have _deploy_resource call on _deploy_workspace
        stack_api._deploy_workspace = mock.MagicMock()
        test_workspace_databricks_id = {api.WORKSPACE_RESOURCE_PATH: '/test/path'}
        stack_api._deploy_workspace.return_value = (test_workspace_databricks_id, {})
        test_workspace_resource_status = {api.RESOURCE_DATABRICKS_ID: test_workspace_databricks_id}
        stack_api._deploy_resource(TEST_WORKSPACE_NB_RESOURCE,
                                   resource_status=test_workspace_resource_status,
                                   overwrite=True)
        stack_api._deploy_workspace.assert_called()
        assert stack_api._deploy_workspace.call_args[0][0] == \
            TEST_WORKSPACE_NB_RESOURCE[api.RESOURCE_PROPERTIES]
        assert stack_api._deploy_workspace.call_args[0][1] == test_workspace_databricks_id

        # A dbfs resource should have _deploy_resource call on _deploy_workspace
        stack_api._deploy_dbfs = mock.MagicMock()
        stack_api._deploy_dbfs.return_value = (TEST_DBFS_FILE_DATABRICKS_ID, {})
        stack_api._deploy_resource(TEST_DBFS_FILE_RESOURCE,
                                   resource_status=TEST_DBFS_FILE_STATUS,
                                   overwrite_dbfs=True)
        stack_api._deploy_dbfs.assert_called()
        assert stack_api._deploy_dbfs.call_args[0][0] == \
            TEST_DBFS_FILE_RESOURCE[api.RESOURCE_PROPERTIES]
        assert stack_api._deploy_dbfs.call_args[0][1] == \
            TEST_DBFS_FILE_STATUS[api.RESOURCE_DATABRICKS_ID]

        # If there is a nonexistent type, raise a StackError.
        resource_badtype = {
            api.RESOURCE_SERVICE: 'nonexist',
            api.RESOURCE_ID: 'test',
            api.RESOURCE_PROPERTIES: {'test': 'test'}
        }
        with pytest.raises(StackError):
            stack_api._deploy_resource(resource_badtype)

    def test_download_workspace(self, stack_api, tmpdir):
        """
            stack_api._download_workspace should call certain workspace client functions depending
            on object_type and error when object_type is defined incorrectly.
        """
        test_deploy_output = {'test': 'test'}  # default deploy_output return value

        stack_api.workspace_client.client = mock.MagicMock()
        stack_api.workspace_client.client.get_status.return_value = test_deploy_output
        stack_api.workspace_client.export_workspace = mock.MagicMock()
        stack_api.workspace_client.export_workspace_dir = mock.MagicMock()

        test_workspace_nb_properties = TEST_WORKSPACE_NB_PROPERTIES.copy()
        test_workspace_nb_properties.update(
            {api.WORKSPACE_RESOURCE_SOURCE_PATH: os.path.join(tmpdir.strpath,
                                                              test_workspace_nb_properties[
                                                                  api.WORKSPACE_RESOURCE_SOURCE_PATH
                                                              ])})
        test_workspace_dir_properties = TEST_WORKSPACE_DIR_PROPERTIES.copy()
        test_workspace_dir_properties.update(
            {api.WORKSPACE_RESOURCE_SOURCE_PATH: os.path.join(tmpdir.strpath,
                                                              test_workspace_dir_properties[
                                                                  api.WORKSPACE_RESOURCE_SOURCE_PATH
                                                              ])})

        stack_api._download_workspace(test_workspace_dir_properties, True)
        stack_api.workspace_client.export_workspace_dir.assert_called_once()
        assert stack_api.workspace_client.export_workspace_dir.call_args[0][0] == \
            test_workspace_dir_properties[api.WORKSPACE_RESOURCE_PATH]
        assert stack_api.workspace_client.export_workspace_dir.call_args[0][1] == \
            test_workspace_dir_properties[api.WORKSPACE_RESOURCE_SOURCE_PATH]

        stack_api._download_workspace(test_workspace_nb_properties, True)
        stack_api.workspace_client.export_workspace.assert_called_once()
        created_dir = os.path.dirname(
            os.path.abspath(test_workspace_nb_properties[api.WORKSPACE_RESOURCE_SOURCE_PATH]))
        assert os.path.exists(created_dir)
        assert stack_api.workspace_client.export_workspace.call_args[0][0] == \
            test_workspace_nb_properties[api.WORKSPACE_RESOURCE_PATH]
        assert stack_api.workspace_client.export_workspace.call_args[0][1] == \
            test_workspace_nb_properties[api.WORKSPACE_RESOURCE_SOURCE_PATH]

        # Should raise error if object_type is not NOTEBOOK or DIRECTORY
        test_workspace_dir_properties.update({api.WORKSPACE_RESOURCE_OBJECT_TYPE: 'INVALID_TYPE'})
        with pytest.raises(StackError):
            stack_api._download_workspace(test_workspace_dir_properties, True)

    def test_download_resource(self, stack_api):
        """
           stack_api._download_resource should correctly call on a specific resource's download
           function.
        """
        # A workspace resource should have _download_resource call on _download_workspace
        stack_api._download_workspace = mock.MagicMock()
        stack_api._download_resource(TEST_WORKSPACE_NB_RESOURCE, overwrite=True)
        stack_api._download_workspace.assert_called()
        assert stack_api._download_workspace.call_args[0][0] == \
            TEST_WORKSPACE_NB_RESOURCE[api.RESOURCE_PROPERTIES]
        assert stack_api._download_workspace.call_args[0][1] is True  # overwrite argument

        # If there is a nonexistent service, StackError shouldn't be raised, since it is intentional
        # that some resource services cannot be downloaded, like jobs.
        resource_badservice = {
            api.RESOURCE_SERVICE: 'nonexist',
            api.RESOURCE_ID: 'test',
            api.RESOURCE_PROPERTIES: {'test': 'test'}
        }
        stack_api._download_resource(resource_badservice)

    def test_deploy_config(self, stack_api, tmpdir):
        """
            The stack status generated from a correctly set up stack passed through deployment
            in stack_api should pass the validation assertions within the deployment procedure
            along with passing some correctness criteria that will be tested here.
        """
        test_deploy_output = {'test': 'test'}
        # Setup mocks for job resource deployment
        stack_api._update_job = mock.MagicMock()
        stack_api._update_job.return_value = 12345
        stack_api._put_job = mock.MagicMock()
        stack_api._put_job.return_value = 12345
        stack_api.jobs_client.get_job = mock.MagicMock()
        stack_api.jobs_client.get_job.return_value = test_deploy_output

        # Setup mocks for workspace resource deployment
        stack_api.workspace_client.import_workspace = mock.MagicMock()
        stack_api.workspace_client.import_workspace_dir = mock.MagicMock()
        stack_api.workspace_client.client.get_status = mock.MagicMock()
        stack_api.workspace_client.client.get_status.return_value = test_deploy_output

        # Setup mocks for dbfs resource deployment
        stack_api.dbfs_client.cp = mock.MagicMock()
        stack_api.dbfs_client.client = mock.MagicMock()
        stack_api.dbfs_client.client.get_status.return_value = test_deploy_output

        # Create files and directories associated with workspace and dbfs resources to ensure
        # that validations within resource-specific services pass.
        test_stack = copy.deepcopy(TEST_STACK)
        for resource in test_stack[api.STACK_RESOURCES]:
            resource_service = resource[api.RESOURCE_SERVICE]
            resource_properties = resource[api.RESOURCE_PROPERTIES]
            curr_source_path = resource_properties.get(api.DBFS_RESOURCE_SOURCE_PATH, '')
            resource_properties.update(
                {api.DBFS_RESOURCE_SOURCE_PATH: os.path.join(tmpdir.strpath, curr_source_path)})
            if resource_service == api.WORKSPACE_SERVICE:
                if workspace_api.NOTEBOOK == \
                        resource_properties[api.WORKSPACE_RESOURCE_OBJECT_TYPE]:
                    os.makedirs(os.path.dirname(resource_properties[
                                                api.WORKSPACE_RESOURCE_SOURCE_PATH]))
                    with open(resource_properties[api.WORKSPACE_RESOURCE_SOURCE_PATH], 'w') as f:
                        f.write("print('test')\n")
                if resource_properties[api.WORKSPACE_RESOURCE_OBJECT_TYPE] == \
                        workspace_api.DIRECTORY:
                    os.makedirs(resource_properties[api.WORKSPACE_RESOURCE_SOURCE_PATH])
            elif resource_service == api.DBFS_SERVICE:
                if resource_properties[api.DBFS_RESOURCE_IS_DIR]:
                    os.makedirs(resource_properties[api.DBFS_RESOURCE_SOURCE_PATH])
                else:
                    with open(resource_properties[api.DBFS_RESOURCE_SOURCE_PATH], 'w') as f:
                        f.write("print('test')\n")

        new_stack_status_1 = stack_api.deploy(test_stack)
        test_job_status_1 = {
            api.RESOURCE_ID: TEST_RESOURCE_ID,
            api.RESOURCE_SERVICE: api.JOBS_SERVICE,
            api.RESOURCE_DATABRICKS_ID: {"job_id": 12345}
        }
        test_stack_status_1 = {
            api.STACK_NAME: "test-stack",
            api.CLI_VERSION_KEY: CLI_VERSION,
            api.STACK_DEPLOYED: [test_job_status_1,
                                 TEST_WORKSPACE_NB_STATUS,
                                 TEST_WORKSPACE_DIR_STATUS,
                                 TEST_DBFS_FILE_STATUS,
                                 TEST_DBFS_DIR_STATUS,
                                 ]
        }
        assert new_stack_status_1 == test_stack_status_1

        # stack_api.deploy should create a valid stack status when given an existing
        # stack_status
        new_stack_status_2 = stack_api.deploy(test_stack, stack_status=TEST_STATUS)
        test_stack_status_2 = TEST_STATUS
        assert new_stack_status_2 == test_stack_status_2
