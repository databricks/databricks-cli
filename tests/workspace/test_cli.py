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
import pytest
from click.testing import CliRunner

import databricks_cli.workspace.cli as cli
import databricks_cli.workspace.api as api
from databricks_cli.workspace.api import WorkspaceFileInfo, NOTEBOOK
from databricks_cli.workspace.types import WorkspaceLanguage
from tests.utils import provide_conf


@pytest.fixture()
def workspace_api_mock():
    with mock.patch('databricks_cli.workspace.cli.WorkspaceApi') as WorkspaceApiMock:
        workspace_api_mock = mock.MagicMock()
        WorkspaceApiMock.return_value = workspace_api_mock
        yield workspace_api_mock


@provide_conf
def test_export_workspace_cli(workspace_api_mock, tmpdir):
    path = tmpdir.strpath
    workspace_api_mock.get_status.return_value = \
        WorkspaceFileInfo('/notebook-name', NOTEBOOK, WorkspaceLanguage.SCALA)
    runner = CliRunner()
    runner.invoke(cli.export_workspace_cli, ['--format', 'SOURCE', '/notebook-name', path])
    assert workspace_api_mock.export_workspace.call_args[0][1] == \
           os.path.join(path, 'notebook-name.scala')


def test_export_dir_helper(workspace_api_mock, tmpdir):
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
    def _list_objects_mock(path):
        if path == '/':
            return [
                WorkspaceFileInfo('/a', api.DIRECTORY),
                WorkspaceFileInfo('/f', api.DIRECTORY)
            ]
        elif path == '/a':
            return [
                WorkspaceFileInfo('/a/b', api.NOTEBOOK, WorkspaceLanguage.SCALA),
                WorkspaceFileInfo('/a/c', api.NOTEBOOK, WorkspaceLanguage.PYTHON),
                WorkspaceFileInfo('/a/d', api.NOTEBOOK, WorkspaceLanguage.R),
                WorkspaceFileInfo('/a/e', api.NOTEBOOK, WorkspaceLanguage.SQL),
            ]
        elif path == '/f':
            return [WorkspaceFileInfo('/f/g', api.DIRECTORY)]
        elif path == '/f/g':
            return []
        else:
            assert False, 'We shouldn\'t reach this case...'

    workspace_api_mock.list_objects = mock.Mock(wraps=_list_objects_mock)
    cli._export_dir_helper(workspace_api_mock, '/', tmpdir.strpath, False)
    # Verify that the directories a, f, g exist.
    assert os.path.isdir(os.path.join(tmpdir.strpath, 'a'))
    assert os.path.isdir(os.path.join(tmpdir.strpath, 'f'))
    assert os.path.isdir(os.path.join(tmpdir.strpath, 'f', 'g'))
    # Verify we exported files b, c, d, e with the correct names
    assert workspace_api_mock.export_workspace.call_count == 4
    assert workspace_api_mock.export_workspace.call_args_list[0][0][0] == '/a/b'
    assert workspace_api_mock.export_workspace.call_args_list[0][0][1] == os.path.join(tmpdir.strpath, 'a', 'b.scala')
    assert workspace_api_mock.export_workspace.call_args_list[1][0][0] == '/a/c'
    assert workspace_api_mock.export_workspace.call_args_list[1][0][1] == os.path.join(tmpdir.strpath, 'a', 'c.py')
    assert workspace_api_mock.export_workspace.call_args_list[2][0][0] == '/a/d'
    assert workspace_api_mock.export_workspace.call_args_list[2][0][1] == os.path.join(tmpdir.strpath, 'a', 'd.r')
    assert workspace_api_mock.export_workspace.call_args_list[3][0][0] == '/a/e'
    assert workspace_api_mock.export_workspace.call_args_list[3][0][1] == os.path.join(tmpdir.strpath, 'a', 'e.sql')
    # Verify that we only called list 4 times.
    assert workspace_api_mock.list_objects.call_count == 4

def test_import_dir_helper(workspace_api_mock, tmpdir):
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
    cli._import_dir_helper(workspace_api_mock, tmpdir.strpath, '/', False, False)
    # Verify that the directories a, f, g exist.
    assert workspace_api_mock.mkdirs.call_count == 4
    # The order of list may be undeterminstic apparently. (It's different in Travis CI)
    assert any([ca[0][0] == '/' for ca in workspace_api_mock.mkdirs.call_args_list])
    assert any([ca[0][0] == '/a' for ca in workspace_api_mock.mkdirs.call_args_list])
    assert any([ca[0][0] == '/f' for ca in workspace_api_mock.mkdirs.call_args_list])
    assert any([ca[0][0] == '/f/g' for ca in workspace_api_mock.mkdirs.call_args_list])
    # Verify that we imported the correct files
    assert workspace_api_mock.import_workspace.call_count == 4
    assert any([ca[0][0] == os.path.join(tmpdir.strpath, 'a', 'b.scala') \
            for ca in workspace_api_mock.import_workspace.call_args_list])
    assert any([ca[0][0] == os.path.join(tmpdir.strpath, 'a', 'c.py') \
            for ca in workspace_api_mock.import_workspace.call_args_list])
    assert any([ca[0][0] == os.path.join(tmpdir.strpath, 'a', 'd.r') \
            for ca in workspace_api_mock.import_workspace.call_args_list])
    assert any([ca[0][0] == os.path.join(tmpdir.strpath, 'a', 'e.sql') \
            for ca in workspace_api_mock.import_workspace.call_args_list])
    assert any([ca[0][1] == '/a/b' for ca in workspace_api_mock.import_workspace.call_args_list])
    assert any([ca[0][1] == '/a/c' for ca in workspace_api_mock.import_workspace.call_args_list])
    assert any([ca[0][1] == '/a/d' for ca in workspace_api_mock.import_workspace.call_args_list])
    assert any([ca[0][1] == '/a/e' for ca in workspace_api_mock.import_workspace.call_args_list])
    assert any([ca[0][2] == WorkspaceLanguage.SCALA \
            for ca in workspace_api_mock.import_workspace.call_args_list])
    assert any([ca[0][2] == WorkspaceLanguage.PYTHON \
            for ca in workspace_api_mock.import_workspace.call_args_list])
    assert any([ca[0][2] == WorkspaceLanguage.R \
            for ca in workspace_api_mock.import_workspace.call_args_list])
    assert any([ca[0][2] == WorkspaceLanguage.SQL \
            for ca in workspace_api_mock.import_workspace.call_args_list])


def test_import_dir_rstrip(workspace_api_mock, tmpdir):
    """
    Copy from directory ``tmpdir`` with structure as follows
    - a (directory)
      - test-py.py (python)
    """
    os.makedirs(os.path.join(tmpdir.strpath, 'a'))
    with open(os.path.join(tmpdir.strpath, 'a', 'test-py.py'), 'wt'):
        pass
    cli._import_dir_helper(workspace_api_mock, tmpdir.strpath, '/', False, False)
    assert workspace_api_mock.mkdirs.call_count == 2
    assert any([ca[0][0] == '/' for ca in workspace_api_mock.mkdirs.call_args_list])
    assert any([ca[0][0] == '/a' for ca in workspace_api_mock.mkdirs.call_args_list])

    # Verify that we imported the correct files with the right names
    assert workspace_api_mock.import_workspace.call_count == 1
    assert any([ca[0][0] == os.path.join(tmpdir.strpath, 'a', 'test-py.py') \
            for ca in workspace_api_mock.import_workspace.call_args_list])
    assert any([ca[0][1] == '/a/test-py' for ca in workspace_api_mock.import_workspace.call_args_list])

def test_import_dir_hidden(workspace_api_mock, tmpdir):
    """
    Copy from directory ``tmpdir`` with structure as follows
    - a (directory)
      - test-py.py (python)
    - .ignore (directory)
      - ignore.py
    """
    os.makedirs(os.path.join(tmpdir.strpath, 'a'))
    os.makedirs(os.path.join(tmpdir.strpath, '.ignore'))
    with open(os.path.join(tmpdir.strpath, 'a', 'test-py.py'), 'wb'):
        pass
    with open(os.path.join(tmpdir.strpath, '.ignore', 'ignore.py'), 'wb'):
        pass
    cli._import_dir_helper(workspace_api_mock, tmpdir.strpath, '/', False, True)
    assert workspace_api_mock.mkdirs.call_count == 2
    assert any([ca[0][0] == '/' for ca in workspace_api_mock.mkdirs.call_args_list])
    assert any([ca[0][0] == '/a' for ca in workspace_api_mock.mkdirs.call_args_list])

    # Verify that we imported the correct files with the right names
    assert workspace_api_mock.import_workspace.call_count == 1
    assert any([ca[0][0] == os.path.join(tmpdir.strpath, 'a', 'test-py.py') \
            for ca in workspace_api_mock.import_workspace.call_args_list])
    assert any([ca[0][1] == '/a/test-py' for ca in workspace_api_mock.import_workspace.call_args_list])
