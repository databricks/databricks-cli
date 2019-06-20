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

import databricks_cli.pipelines.api as api
from databricks_cli.pipelines.api import LibraryObject

DEPLOY_SPEC = {'id': '123'}
PIPELINE_ID = '123'
CREDENTIALS = 'dummy_credentials'
HEADERS = 'dummy_headers'


@pytest.fixture()
def pipelines_api():
    with mock.patch('databricks_cli.pipelines.api.DeltaPipelinesService') as DeltaPipelinesServiceMock:
        DeltaPipelinesServiceMock.return_value = mock.MagicMock()
        _pipelines_api = api.PipelinesApi(None)
        yield _pipelines_api


def test_partition_correctly_identifies_local_paths(pipelines_api):
    libraries = [
        LibraryObject('jar', '//absolute/path/abc.ext'),
        LibraryObject('jar', '/relative/path.ext'),
        LibraryObject('jar', 'file://file/scheme/abs/path.ext'),
        LibraryObject('jar', 'file:/file/scheme/relative/path.ext'),
        LibraryObject('jar', 'FILE:/all/caps.ext'),
        LibraryObject('jar', 'FiLe:/weird/case.ext'),
        LibraryObject('jar', 'file.ext')
    ]
    expected_llo = [
        LibraryObject('jar', '//absolute/path/abc.ext'),
        LibraryObject('jar', '/relative/path.ext'),
        LibraryObject('jar', '//file/scheme/abs/path.ext'),
        LibraryObject('jar', '/file/scheme/relative/path.ext'),
        LibraryObject('jar', '/all/caps.ext'),
        LibraryObject('jar', '/weird/case.ext'),
        LibraryObject('jar', 'file.ext')
    ]
    expected_rest = []
    llo, rest = pipelines_api._partition_libraries_and_extract_local_paths(libraries)
    assert len(llo) == len(expected_llo)
    for i in range(len(llo)):
        assert llo[i].lib_type == expected_llo[i].lib_type
        assert llo[i].path == expected_llo[i].path

    assert len(rest) == len(expected_rest)
    for i in range(len(rest)):
        assert rest[i].lib_type == expected_rest[i].lib_type
        assert rest[i].path == expected_rest[i].path


def test_partition_correctly_identifies_remote_paths(pipelines_api):
    libraries = [
        LibraryObject('jar', 'dbfs:/absolute/path/abc.ext'),
        LibraryObject('jar', 's3:file://file/scheme/abs/path.ext'),
        LibraryObject('jar', 's3:file:/file/scheme/relative/path.ext'),
        LibraryObject('jar', 'scheme:file.ext'),
        LibraryObject('jar', 'scheme://abs/path.ext'),
        LibraryObject('jar', 'scheme:/rel/path.ext'),
        LibraryObject('egg', 'file:/rel/path.ext'),
        LibraryObject('whl', '/rel/path.ext')
    ]
    expected_llo = []
    expected_rest = [
        LibraryObject('jar', 'dbfs:/absolute/path/abc.ext'),
        LibraryObject('jar', 's3:file://file/scheme/abs/path.ext'),
        LibraryObject('jar', 's3:file:/file/scheme/relative/path.ext'),
        LibraryObject('jar', 'scheme:file.ext'),
        LibraryObject('jar', 'scheme://abs/path.ext'),
        LibraryObject('jar', 'scheme:/rel/path.ext'),
        LibraryObject('egg', 'file:/rel/path.ext'),
        LibraryObject('whl', '/rel/path.ext')
    ]
    llo, rest = pipelines_api._partition_libraries_and_extract_local_paths(libraries)
    assert len(llo) == len(expected_llo)
    for i in range(len(llo)):
        assert llo[i].lib_type == expected_llo[i].lib_type
        assert llo[i].path == expected_llo[i].path

    assert len(rest) == len(expected_rest)
    for i in range(len(rest)):
        assert rest[i].lib_type == expected_rest[i].lib_type
        assert rest[i].path == expected_rest[i].path


@mock.patch('databricks_cli.dbfs.api.DbfsApi.file_exists', lambda a, b: True)
@mock.patch('databricks_cli.dbfs.dbfs_path.DbfsPath.validate')
def test_get_files_to_upload_none(DbfsPathMock, pipelines_api):
    local_lib_objects = [
        LibraryObject('jar', '//absolute/path/abc.ext'),
        LibraryObject('jar', '/relative/path.ext'),
        LibraryObject('jar', '//file/scheme/abs/path.ext'),
        LibraryObject('jar', '/file/scheme/relative/path.ext')
    ]
    remote_lib_objects = [
        LibraryObject('jar', 'dbfs:/pipeilnes/code/absolute/path/abc.ext'),
        LibraryObject('jar', 'dbfs:/pipeilnes/code/relative/path.ext'),
        LibraryObject('jar', 'dbfs:/pipeilnes/code//file/scheme/abs/path.ext'),
        LibraryObject('jar', 'dbfs:/pipeilnes/code/file/scheme/relative/path.ext')
    ]
    llo_tuples = pipelines_api._get_files_to_upload(local_lib_objects, remote_lib_objects)
    assert len(llo_tuples) == 0


@mock.patch('databricks_cli.dbfs.api.DbfsApi.file_exists', lambda a, b: False)
@mock.patch('databricks_cli.dbfs.dbfs_path.DbfsPath.validate')
def test_get_files_to_upload_all(DbfsPathMock, pipelines_api):
    local_lib_objects = [
        LibraryObject('jar', '/absolute/path/abc.ext'),
        LibraryObject('jar', '/relative/path.ext'),
        LibraryObject('jar', '//file/scheme/abs/path.ext'),
        LibraryObject('jar', '/file/scheme/relative/path.ext')
    ]
    remote_lib_objects = [
        LibraryObject('jar', 'dbfs:/pipeilnes/code//absolute/path/abc.ext'),
        LibraryObject('jar', 'dbfs:/pipeilnes/code/relative/path.ext'),
        LibraryObject('jar', 'dbfs:/pipeilnes/code//file/scheme/abs/path.ext'),
        LibraryObject('jar', 'dbfs:/pipeilnes/code/file/scheme/relative/path.ext')
    ]
    llo_tuples = pipelines_api._get_files_to_upload(local_lib_objects, remote_lib_objects)
    assert len(llo_tuples) == 4




@mock.patch('databricks_cli.pipelines.api.PipelinesApi._get_credentials_for_request')
def test_delete_no_headers(get_credentials_mock, pipelines_api):
    get_credentials_mock.return_value = CREDENTIALS
    pipelines_api.delete(PIPELINE_ID)
    delete_mock = pipelines_api.client.delete
    assert get_credentials_mock.call_count == 1
    assert delete_mock.call_count == 1
    assert delete_mock.call_args[0][0] == PIPELINE_ID
    assert delete_mock.call_args[0][1] == CREDENTIALS
    assert delete_mock.call_args[0][2] is None


@mock.patch('databricks_cli.pipelines.api.PipelinesApi._get_credentials_for_request')
def test_delete_headers(get_credentials_mock, pipelines_api):
    get_credentials_mock.return_value = CREDENTIALS
    pipelines_api.delete(PIPELINE_ID, HEADERS)
    delete_mock = pipelines_api.client.delete
    assert get_credentials_mock.call_count == 1
    assert delete_mock.call_count == 1
    assert delete_mock.call_args[0][0] == PIPELINE_ID
    assert delete_mock.call_args[0][1] == CREDENTIALS
    assert delete_mock.call_args[0][2] == HEADERS
