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
# pylint:disable=unused-argument

import copy
import mock
import pytest

import databricks_cli.pipelines.api as api
from databricks_cli.pipelines.api import LibraryObject

PIPELINE_ID = '123456'
SPEC = {
    'id': PIPELINE_ID,
    'name': 'test_pipeline'
}
CREDENTIALS = 'dummy_credentials'
HEADERS = 'dummy_headers'


@pytest.fixture()
def pipelines_api():
    with mock.patch('databricks_cli.pipelines.api.DeltaPipelinesService') \
            as DeltaPipelinesServiceMock:
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
    assert llo == expected_llo
    assert rest == expected_rest


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
    assert llo == expected_llo
    assert rest == expected_rest


def test_library_object_serialization_deserialization():
    libraries = [
        {'jar': '//absolute/path/abc.ext'},
        {'jar': '/relative/path.ext'},
        {'jar': 'file://file/scheme/abs/path.ext'},
        {'jar': 'file:/file/scheme/relative/path.ext'},
        {'jar': 'FILE:/all/caps.ext'},
        {'egg': 'FiLe:/weird/case.ext'},
        {'whl': 'file.ext'},
        {'whl': 's3:/s3/path/file.ext'},
        {'jar': 'dbfs:/dbfs/path/file.ext'}
    ]
    library_objects = [
        LibraryObject('jar', '//absolute/path/abc.ext'),
        LibraryObject('jar', '/relative/path.ext'),
        LibraryObject('jar', 'file://file/scheme/abs/path.ext'),
        LibraryObject('jar', 'file:/file/scheme/relative/path.ext'),
        LibraryObject('jar', 'FILE:/all/caps.ext'),
        LibraryObject('egg', 'FiLe:/weird/case.ext'),
        LibraryObject('whl', 'file.ext'),
        LibraryObject('whl', 's3:/s3/path/file.ext'),
        LibraryObject('jar', 'dbfs:/dbfs/path/file.ext')
    ]
    llo = LibraryObject.convert_from_libraries(libraries)
    assert llo == library_objects

    libs = LibraryObject.convert_to_libraries(library_objects)
    assert libs == libraries


def file_exists_stub(self, dbfs_path):
    exist_mapping = {
        'dbfs:/pipelines/code/40bd001563085fc35165329ea1ff5c5ecbdbbeef.jar': True,  # sha1 of 123
        'dbfs:/pipelines/code/51eac6b471a284d3341d8c0c63d0f1a286262a18.jar': False,  # sha1 of 456
    }
    return exist_mapping[dbfs_path.absolute_path]


@mock.patch('databricks_cli.dbfs.api.DbfsApi.file_exists', file_exists_stub)
@mock.patch('databricks_cli.dbfs.dbfs_path.DbfsPath.validate')
@mock.patch('databricks_cli.pipelines.api.PipelinesApi._get_credentials_for_request')
@mock.patch('databricks_cli.dbfs.api.DbfsApi.put_file')
def test_deploy(put_file_mock, get_credentials_mock, dbfs_path_validate, pipelines_api, tmpdir):
    get_credentials_mock.return_value = CREDENTIALS
    deploy_mock = pipelines_api.client.client.perform_query
    # set-up the test
    jar1 = tmpdir.join('/jar1.jar').strpath
    jar2 = tmpdir.join('/jar2.jar').strpath
    with open(jar1, 'w') as f:
        f.write('123')
    with open(jar2, 'w') as f:
        f.write('456')
    libraries = [{'jar': jar1}, {'jar': jar2}, {'jar': 'dbfs:/pipelines/code/file.jar'}]
    SPEC['libraries'] = libraries
    expected_spec = copy.deepcopy(SPEC)
    expected_spec['libraries'] = [
        {'jar': 'dbfs:/pipelines/code/file.jar'},
        {'jar': 'dbfs:/pipelines/code/40bd001563085fc35165329ea1ff5c5ecbdbbeef.jar'},
        {'jar': 'dbfs:/pipelines/code/51eac6b471a284d3341d8c0c63d0f1a286262a18.jar'},
    ]
    expected_spec['credentials'] = CREDENTIALS

    pipelines_api.deploy(SPEC)
    assert dbfs_path_validate.call_count == 2
    assert put_file_mock.call_count == 1
    assert put_file_mock.call_args[0][0] == jar2
    assert put_file_mock.call_args[0][1].absolute_path ==\
        'dbfs:/pipelines/code/51eac6b471a284d3341d8c0c63d0f1a286262a18.jar'
    deploy_mock.assert_called_with('PUT', '/pipelines/{}'.format(PIPELINE_ID),
                                   data=expected_spec, headers=None)

    pipelines_api.deploy(SPEC, HEADERS)
    deploy_mock.assert_called_with('PUT', '/pipelines/{}'.format(PIPELINE_ID),
                                   data=expected_spec, headers=HEADERS)


@mock.patch('databricks_cli.pipelines.api.PipelinesApi._get_credentials_for_request')
def test_delete(get_credentials_mock, pipelines_api):
    get_credentials_mock.return_value = CREDENTIALS
    pipelines_api.delete(PIPELINE_ID)
    delete_mock = pipelines_api.client.delete
    assert get_credentials_mock.call_count == 1
    assert delete_mock.call_count == 1
    assert delete_mock.call_args[0][0] == PIPELINE_ID
    assert delete_mock.call_args[0][1] == CREDENTIALS
    assert delete_mock.call_args[0][2] is None

    pipelines_api.delete(PIPELINE_ID, HEADERS)
    assert delete_mock.call_args[0][0] == PIPELINE_ID
    assert delete_mock.call_args[0][1] == CREDENTIALS
    assert delete_mock.call_args[0][2] == HEADERS
