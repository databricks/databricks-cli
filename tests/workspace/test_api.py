
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

import pytest

import databricks_cli.workspace.api as api
from databricks_cli.workspace.api import WorkspaceFileInfo
from databricks_cli.workspace.types import WorkspaceLanguage

TEST_WORKSPACE_PATH = '/test/workspace/path'
TEST_WORKSPACE_OBJECT_ID = '22'
TEST_JSON_RESPONSE = {
    'path': TEST_WORKSPACE_PATH,
    'object_type': api.DIRECTORY,
    'created_at': 124,
    'object_id': TEST_WORKSPACE_OBJECT_ID,
}
TEST_LANGUAGE = 'PYTHON'
TEST_FMT = 'SOURCE'


class TestWorkspaceFileInfo(object):
    def test_to_row_not_long_form_not_absolute(self):
        file_info = api.WorkspaceFileInfo(TEST_WORKSPACE_PATH, api.NOTEBOOK,
                                          TEST_WORKSPACE_OBJECT_ID)
        row = file_info.to_row(is_long_form=False, is_absolute=False)
        assert len(row) == 1
        assert row[0] == file_info.basename

    def test_to_row_not_long_form_absolute(self):
        file_info = api.WorkspaceFileInfo(TEST_WORKSPACE_PATH, api.NOTEBOOK,
                                          TEST_WORKSPACE_OBJECT_ID)
        row = file_info.to_row(is_long_form=False, is_absolute=True)
        assert len(row) == 1
        assert row[0] == TEST_WORKSPACE_PATH

    def test_to_row_long_form_absolute(self):
        file_info = api.WorkspaceFileInfo(TEST_WORKSPACE_PATH, api.NOTEBOOK,
                                          TEST_WORKSPACE_OBJECT_ID)
        row = file_info.to_row(is_long_form=True, is_absolute=True)
        assert len(row) == 3
        assert row[0] == api.NOTEBOOK
        assert row[1] == TEST_WORKSPACE_PATH
        assert row[2] is None

    def test_basename(self):
        file_info = api.WorkspaceFileInfo(TEST_WORKSPACE_PATH, api.NOTEBOOK,
                                          TEST_WORKSPACE_OBJECT_ID)
        assert file_info.basename == 'path'

    def test_from_json(self):
        file_info = api.WorkspaceFileInfo.from_json(TEST_JSON_RESPONSE)
        assert file_info.path == TEST_WORKSPACE_PATH


@pytest.fixture()
def workspace_api():
    with mock.patch('databricks_cli.workspace.api.WorkspaceService') as WorkspaceServiceMock:
        WorkspaceServiceMock.return_value = mock.MagicMock()
        workspace_api = api.WorkspaceApi(None)
        yield workspace_api


class TestWorkspaceApi(object):
    def test_get_status(self, workspace_api):
        workspace_api.client.get_status.return_value = TEST_JSON_RESPONSE
        file_info = workspace_api.get_status(TEST_WORKSPACE_PATH)
        assert file_info.path == TEST_WORKSPACE_PATH

    def test_list_objects(self, workspace_api):
        workspace_api.client.list.return_value = {'objects': [TEST_JSON_RESPONSE]}
        files = workspace_api.list_objects(TEST_WORKSPACE_PATH)
        assert len(files) == 1
        assert files[0].path == TEST_WORKSPACE_PATH
        # Test case where API returns {}
        workspace_api.client.list.return_value = {}
        files = workspace_api.list_objects(TEST_WORKSPACE_PATH)
        assert len(files) == 0

    def test_mkdirs(self, workspace_api):
        workspace_api.mkdirs(TEST_WORKSPACE_PATH)
        mkdirs_mock = workspace_api.client.mkdirs
        assert mkdirs_mock.call_count == 1
        assert mkdirs_mock.call_args[0][0] == TEST_WORKSPACE_PATH

    def test_import_workspace(self, workspace_api, tmpdir):
        test_file_path = os.path.join(tmpdir.strpath, 'test')
        with open(test_file_path, 'w') as f:
            f.write('test')
        workspace_api.import_workspace(test_file_path, TEST_WORKSPACE_PATH, TEST_LANGUAGE,
                                       TEST_FMT, is_overwrite=False)
        import_workspace_mock = workspace_api.client.import_workspace
        assert import_workspace_mock.call_count == 1
        assert import_workspace_mock.call_args[0][0] == TEST_WORKSPACE_PATH
        assert import_workspace_mock.call_args[0][1] == TEST_FMT
        assert import_workspace_mock.call_args[0][2] == TEST_LANGUAGE
        assert import_workspace_mock.call_args[0][3] == b64encode(b'test').decode()
        assert import_workspace_mock.call_args[0][4] is False

    def test_export_workspace(self, workspace_api, tmpdir):
        test_file_path = os.path.join(tmpdir.strpath, 'test')
        workspace_api.client.export_workspace.return_value = {'content': b64encode(b'test')}
        workspace_api.export_workspace(TEST_WORKSPACE_PATH, test_file_path, TEST_FMT,
                                       is_overwrite=False)
        with open(test_file_path, 'r') as f:
            contents = f.read()
            assert contents == 'test'

    def test_delete(self, workspace_api):
        workspace_api.delete(TEST_WORKSPACE_PATH, is_recursive=True)
        delete_mock = workspace_api.client.delete
        assert delete_mock.call_count == 1
        assert delete_mock.call_args[0][0] == TEST_WORKSPACE_PATH
        assert delete_mock.call_args[0][1] is True

    def test_export_workspace_dir(self, workspace_api, tmpdir):
        """
            Copy to directory ``tmpdir`` with structure as follows
            - a (directory)
              - b (scala)
              - c (python)
              - d (r)
              - e (sql)
            - f (directory)
              - g (directory)
            """

        workspace_api.export_workspace = mock.MagicMock()

        def _list_objects_mock(path, headers=None):
            if path == '/':
                return [
                    WorkspaceFileInfo('/a', api.DIRECTORY, TEST_WORKSPACE_OBJECT_ID),
                    WorkspaceFileInfo('/f', api.DIRECTORY, TEST_WORKSPACE_OBJECT_ID)
                ]
            elif path == '/a':
                return [
                    WorkspaceFileInfo('/a/b', api.NOTEBOOK, TEST_WORKSPACE_OBJECT_ID,
                                      WorkspaceLanguage.SCALA),
                    WorkspaceFileInfo('/a/c', api.NOTEBOOK, TEST_WORKSPACE_OBJECT_ID,
                                      WorkspaceLanguage.PYTHON),
                    WorkspaceFileInfo('/a/d', api.NOTEBOOK, TEST_WORKSPACE_OBJECT_ID,
                                      WorkspaceLanguage.R),
                    WorkspaceFileInfo('/a/e', api.NOTEBOOK, TEST_WORKSPACE_OBJECT_ID,
                                      WorkspaceLanguage.SQL),
                ]
            elif path == '/f':
                return [WorkspaceFileInfo('/f/g', api.DIRECTORY, TEST_WORKSPACE_OBJECT_ID)]
            elif path == '/f/g':
                return []
            else:
                assert False, 'We shouldn\'t reach this case...'

        workspace_api.list_objects = mock.Mock(wraps=_list_objects_mock)
        workspace_api.export_workspace_dir('/', tmpdir.strpath, False)
        # Verify that the directories a, f, g exist.
        assert os.path.isdir(os.path.join(tmpdir.strpath, 'a'))
        assert os.path.isdir(os.path.join(tmpdir.strpath, 'f'))
        assert os.path.isdir(os.path.join(tmpdir.strpath, 'f', 'g'))
        # Verify we exported files b, c, d, e with the correct names
        assert workspace_api.export_workspace.call_count == 4
        assert workspace_api.export_workspace.call_args_list[0][0][0] == '/a/b'
        assert workspace_api.export_workspace.call_args_list[0][0][1] == os.path.join(
            tmpdir.strpath, 'a', 'b.scala')
        assert workspace_api.export_workspace.call_args_list[1][0][0] == '/a/c'
        assert workspace_api.export_workspace.call_args_list[1][0][1] == os.path.join(
            tmpdir.strpath, 'a', 'c.py')
        assert workspace_api.export_workspace.call_args_list[2][0][0] == '/a/d'
        assert workspace_api.export_workspace.call_args_list[2][0][1] == os.path.join(
            tmpdir.strpath, 'a', 'd.r')
        assert workspace_api.export_workspace.call_args_list[3][0][0] == '/a/e'
        assert workspace_api.export_workspace.call_args_list[3][0][1] == os.path.join(
            tmpdir.strpath, 'a', 'e.sql')
        # Verify that we only called list 4 times.
        assert workspace_api.list_objects.call_count == 4

    def test_import_workspace_dir(self, workspace_api, tmpdir):
        """
        Copy from directory ``tmpdir`` with structure as follows
        - a (directory)
          - b.scala (scala)
          - c.py (python)
          - d.r (r)
          - e.sql (sql)
        - f (directory)
          - g (directory)
        """
        workspace_api.import_workspace = mock.MagicMock()
        workspace_api.mkdirs = mock.MagicMock()
        os.makedirs(os.path.join(tmpdir.strpath, 'a'))
        os.makedirs(os.path.join(tmpdir.strpath, 'f'))
        os.makedirs(os.path.join(tmpdir.strpath, 'f', 'g'))
        with open(os.path.join(tmpdir.strpath, 'a', 'b.scala'), 'wt') as f:
            f.write('println(1 + 1)')
        with open(os.path.join(tmpdir.strpath, 'a', 'c.py'), 'wt') as f:
            f.write('print 1 + 1')
        with open(os.path.join(tmpdir.strpath, 'a', 'd.r'), 'wt') as f:
            f.write('I don\'t know how to write r')
        with open(os.path.join(tmpdir.strpath, 'a', 'e.sql'), 'wt') as f:
            f.write('select 1+1 from table;')
        workspace_api.import_workspace_dir(tmpdir.strpath, '/', False, False)
        # Verify that the directories a, f, g exist.
        assert workspace_api.mkdirs.call_count == 4
        # The order of list may be undeterminstic apparently. (It's different in Travis CI)
        assert any([ca[0][0] == '/' for ca in workspace_api.mkdirs.call_args_list])
        assert any([ca[0][0] == '/a' for ca in workspace_api.mkdirs.call_args_list])
        assert any([ca[0][0] == '/f' for ca in workspace_api.mkdirs.call_args_list])
        assert any([ca[0][0] == '/f/g' for ca in workspace_api.mkdirs.call_args_list])
        # Verify that we imported the correct files
        assert workspace_api.import_workspace.call_count == 4
        assert any([ca[0][0] == os.path.join(tmpdir.strpath, 'a', 'b.scala')
                    for ca in workspace_api.import_workspace.call_args_list])
        assert any([ca[0][0] == os.path.join(tmpdir.strpath, 'a', 'c.py')
                    for ca in workspace_api.import_workspace.call_args_list])
        assert any([ca[0][0] == os.path.join(tmpdir.strpath, 'a', 'd.r')
                    for ca in workspace_api.import_workspace.call_args_list])
        assert any([ca[0][0] == os.path.join(tmpdir.strpath, 'a', 'e.sql')
                    for ca in workspace_api.import_workspace.call_args_list])
        assert any([ca[0][1] == '/a/b' for ca in workspace_api.import_workspace.call_args_list])
        assert any([ca[0][1] == '/a/c' for ca in workspace_api.import_workspace.call_args_list])
        assert any([ca[0][1] == '/a/d' for ca in workspace_api.import_workspace.call_args_list])
        assert any([ca[0][1] == '/a/e' for ca in workspace_api.import_workspace.call_args_list])
        assert any([ca[0][2] == WorkspaceLanguage.SCALA
                    for ca in workspace_api.import_workspace.call_args_list])
        assert any([ca[0][2] == WorkspaceLanguage.PYTHON
                    for ca in workspace_api.import_workspace.call_args_list])
        assert any([ca[0][2] == WorkspaceLanguage.R
                    for ca in workspace_api.import_workspace.call_args_list])
        assert any([ca[0][2] == WorkspaceLanguage.SQL
                    for ca in workspace_api.import_workspace.call_args_list])

    def test_import_dir_rstrip(self, workspace_api, tmpdir):
        """
        Copy from directory ``tmpdir`` with structure as follows
        - a (directory)
          - test-py.py (python)
        """

        workspace_api.import_workspace = mock.MagicMock()
        workspace_api.mkdirs = mock.MagicMock()
        os.makedirs(os.path.join(tmpdir.strpath, 'a'))
        with open(os.path.join(tmpdir.strpath, 'a', 'test-py.py'), 'wt'):
            pass
        workspace_api.import_workspace_dir(tmpdir.strpath, '/', False, False)
        assert workspace_api.mkdirs.call_count == 2
        assert any([ca[0][0] == '/' for ca in workspace_api.mkdirs.call_args_list])
        assert any([ca[0][0] == '/a' for ca in workspace_api.mkdirs.call_args_list])

        # Verify that we imported the correct files with the right names
        assert workspace_api.import_workspace.call_count == 1
        assert any([ca[0][0] == os.path.join(tmpdir.strpath, 'a', 'test-py.py')
                    for ca in workspace_api.import_workspace.call_args_list])
        assert any([ca[0][1] == '/a/test-py'
                    for ca in workspace_api.import_workspace.call_args_list])

    def test_import_dir_hidden(self, workspace_api, tmpdir):
        """
        Copy from directory ``tmpdir`` with structure as follows
        - a (directory)
          - test-py.py (python)
        - .ignore (directory)
          - ignore.py
        """
        workspace_api.import_workspace = mock.MagicMock()
        workspace_api.mkdirs = mock.MagicMock()
        workspace_api.import_workspace.return_value = None
        os.makedirs(os.path.join(tmpdir.strpath, 'a'))
        os.makedirs(os.path.join(tmpdir.strpath, '.ignore'))
        with open(os.path.join(tmpdir.strpath, 'a', 'test-py.py'), 'wb'):
            pass
        with open(os.path.join(tmpdir.strpath, '.ignore', 'ignore.py'), 'wb'):
            pass
        workspace_api.import_workspace_dir(tmpdir.strpath, '/', False, True)
        assert workspace_api.mkdirs.call_count == 2
        assert any([ca[0][0] == '/' for ca in workspace_api.mkdirs.call_args_list])
        assert any([ca[0][0] == '/a' for ca in workspace_api.mkdirs.call_args_list])

        # Verify that we imported the correct files with the right names
        assert workspace_api.import_workspace.call_count == 1
        assert any([ca[0][0] == os.path.join(tmpdir.strpath, 'a', 'test-py.py')
                    for ca in workspace_api.import_workspace.call_args_list])
        assert any([ca[0][1] == '/a/test-py'
                    for ca in workspace_api.import_workspace.call_args_list])
