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

import os
import mock
from base64 import b64encode

import databricks_cli.workspace.api as api

TEST_WORKSPACE_PATH = '/test/workspace/path'
TEST_JSON_RESPONSE = {
    'path': TEST_WORKSPACE_PATH,
    'object_type': api.DIRECTORY
}
TEST_LANGUAGE = 'PYTHON'
TEST_FMT = 'SOURCE'


class TestWorkspaceFileInfo(object):
    def test_to_row_not_long_form_not_absolute(self):
        file_info = api.WorkspaceFileInfo(TEST_WORKSPACE_PATH, api.NOTEBOOK)
        row = file_info.to_row(is_long_form=False, is_absolute=False)
        assert len(row) == 1
        assert row[0] == file_info.basename

    def test_to_row_not_long_form_absolute(self):
        file_info = api.WorkspaceFileInfo(TEST_WORKSPACE_PATH, api.NOTEBOOK)
        row = file_info.to_row(is_long_form=False, is_absolute=True)
        assert len(row) == 1
        assert row[0] == TEST_WORKSPACE_PATH

    def test_to_row_long_form_absolute(self):
        file_info = api.WorkspaceFileInfo(TEST_WORKSPACE_PATH, api.NOTEBOOK)
        row = file_info.to_row(is_long_form=True, is_absolute=True)
        assert len(row) == 3
        assert row[0] == api.NOTEBOOK
        assert row[1] == TEST_WORKSPACE_PATH
        assert row[2] is None

    def test_basename(self):
        file_info = api.WorkspaceFileInfo(TEST_WORKSPACE_PATH, api.NOTEBOOK)
        assert file_info.basename == 'path'

    def test_from_json(self):
        file_info = api.WorkspaceFileInfo.from_json(TEST_JSON_RESPONSE)
        assert file_info.path == TEST_WORKSPACE_PATH


def test_get_status():
    with mock.patch('databricks_cli.workspace.api.get_workspace_client') as get_workspace_client:
        get_workspace_client.return_value.get_status.return_value = TEST_JSON_RESPONSE
        file_info = api.get_status(TEST_WORKSPACE_PATH)
        assert file_info.path == TEST_WORKSPACE_PATH


def test_list_objects():
    with mock.patch('databricks_cli.workspace.api.get_workspace_client') as get_workspace_client:
        get_workspace_client.return_value.list.return_value = {'objects': [TEST_JSON_RESPONSE]}
        files = api.list_objects(TEST_WORKSPACE_PATH)
        assert len(files) == 1
        assert files[0].path == TEST_WORKSPACE_PATH
    # Test case where API returns {}
    with mock.patch('databricks_cli.workspace.api.get_workspace_client') as get_workspace_client:
        get_workspace_client.return_value.list.return_value = {}
        files = api.list_objects(TEST_WORKSPACE_PATH)
        assert len(files) == 0


def test_mkdirs():
    with mock.patch('databricks_cli.workspace.api.get_workspace_client') as get_workspace_client:
        api.mkdirs(TEST_WORKSPACE_PATH)
        mkdirs_mock = get_workspace_client.return_value.mkdirs
        assert mkdirs_mock.call_count == 1
        assert mkdirs_mock.call_args[0][0] == TEST_WORKSPACE_PATH


def test_import_workspace(tmpdir):
    with mock.patch('databricks_cli.workspace.api.get_workspace_client') as get_workspace_client:
        test_file_path = os.path.join(tmpdir.strpath, 'test')
        with open(test_file_path, 'w') as f:
            f.write('test')
        api.import_workspace(test_file_path, TEST_WORKSPACE_PATH, TEST_LANGUAGE, TEST_FMT, is_overwrite=False)
        import_workspace_mock = get_workspace_client.return_value.import_workspace
        assert import_workspace_mock.call_count == 1
        assert import_workspace_mock.call_args[0][0] == TEST_WORKSPACE_PATH
        assert import_workspace_mock.call_args[0][1] == TEST_FMT
        assert import_workspace_mock.call_args[0][2] == TEST_LANGUAGE
        assert import_workspace_mock.call_args[0][3] == b64encode('test')
        assert import_workspace_mock.call_args[0][4] == False


def test_export_workspace(tmpdir):
    with mock.patch('databricks_cli.workspace.api.get_workspace_client') as get_workspace_client:
        test_file_path = os.path.join(tmpdir.strpath, 'test')
        get_workspace_client.return_value.export_workspace.return_value = {'content': b64encode('test')}
        api.export_workspace(TEST_WORKSPACE_PATH, test_file_path, TEST_FMT, is_overwrite=False)
        with open(test_file_path, 'r') as f:
            contents = f.read()
            assert contents == 'test'


def test_delete():
    with mock.patch('databricks_cli.workspace.api.get_workspace_client') as get_workspace_client:
        api.delete(TEST_WORKSPACE_PATH, is_recursive=True)
        delete_mock = get_workspace_client.return_value.delete
        assert delete_mock.call_count == 1
        assert delete_mock.call_args[0][0] == TEST_WORKSPACE_PATH
        assert delete_mock.call_args[0][1] == True
