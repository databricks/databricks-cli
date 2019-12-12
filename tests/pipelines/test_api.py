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
#pylint: disable-msg=too-many-locals


import os
import copy
import mock
import pytest

import databricks_cli.pipelines.api as api
from databricks_cli.pipelines.api import LibraryObject

PIPELINE_ID = '123456'
SPEC = {
    'id': PIPELINE_ID,
    'name': 'test_pipeline',
    'storage': 'dbfs:/path'
}
ID_ONLY_SPEC = {
    'pipeline_id': PIPELINE_ID,
}
HEADERS = {'dummy_header': 'dummy_value'}


@pytest.fixture()
def pipelines_api():
    client_mock = mock.MagicMock()

    def server_response(*args, **kwargs):
        if args[0] == 'GET':
            return {'pipeline_id': PIPELINE_ID, 'state': 'RUNNING'}
    client_mock.perform_query = mock.MagicMock(side_effect=server_response)
    _pipelines_api = api.PipelinesApi(client_mock)
    yield _pipelines_api


def file_exists_stub(_, dbfs_path):
    exist_mapping = {
        'dbfs:/pipelines/code/40bd001563085fc35165329ea1ff5c5ecbdbbeef.jar': True,  # sha1 of 123
        'dbfs:/pipelines/code/51eac6b471a284d3341d8c0c63d0f1a286262a18.jar': False,  # sha1 of 456
        'dbfs:/pipelines/code/51eac6b471a284d3341d8c0c63d0f1a286262a18/wheel-name-conv.whl':
            False  # sha1 456
    }
    return exist_mapping[dbfs_path.absolute_path]


@mock.patch('databricks_cli.dbfs.api.DbfsApi.file_exists', file_exists_stub)
@mock.patch('databricks_cli.dbfs.dbfs_path.DbfsPath.validate')
@mock.patch('databricks_cli.dbfs.api.DbfsApi.put_file')
def test_deploy(put_file_mock, dbfs_path_validate, pipelines_api, tmpdir):
    """
    Scenarios Tested:
    1. All three types of local file paths (absolute, relative, file: scheme)
    2. File already present in dbfs
    3. Local files already present in dbfs
    4. Local files that need to be uploaded to dbfs
    Every test local file which has '123' written to it is expected to be already present in Dbfs.
    A test local file which has '456' written to it is not present in Dbfs and therefore must be.
    uploaded to dbfs.
    """
    # set-up the test
    jar1 = tmpdir.join('jar1.jar').strpath
    jar2 = tmpdir.join('jar2.jar').strpath
    jar3 = tmpdir.join('jar3.jar').strpath
    jar4 = tmpdir.join('jar4.jar').strpath
    wheel1 = tmpdir.join('wheel-name-conv.whl').strpath
    jar3_relpath = os.path.relpath(jar3, os.getcwd())
    jar4_file_prefix = 'file:{}'.format(jar4)
    with open(jar1, 'w') as f:
        f.write('123')
    with open(jar2, 'w') as f:
        f.write('456')
    with open(jar3, 'w') as f:
        f.write('456')
    with open(jar4, 'w') as f:
        f.write('456')
    with open(wheel1, 'w') as f:
        f.write('456')
    libraries = [{'jar': 'dbfs:/pipelines/code/file.jar'},
                 {'jar': jar1},
                 {'jar': jar2},
                 {'jar': jar3_relpath},
                 {'jar': jar4_file_prefix},
                 {'whl': wheel1}]
    spec = copy.deepcopy(SPEC)
    spec['libraries'] = libraries
    expected_spec = copy.deepcopy(SPEC)
    expected_spec['libraries'] = [
        {'jar': 'dbfs:/pipelines/code/file.jar'},
        {'jar': 'dbfs:/pipelines/code/40bd001563085fc35165329ea1ff5c5ecbdbbeef.jar'},
        {'jar': 'dbfs:/pipelines/code/51eac6b471a284d3341d8c0c63d0f1a286262a18.jar'},
        {'jar': 'dbfs:/pipelines/code/51eac6b471a284d3341d8c0c63d0f1a286262a18.jar'},
        {'jar': 'dbfs:/pipelines/code/51eac6b471a284d3341d8c0c63d0f1a286262a18.jar'},
        {'whl':
            'dbfs:/pipelines/code/51eac6b471a284d3341d8c0c63d0f1a286262a18/wheel-name-conv.whl'}
    ]

    pipelines_api.deploy(spec)
    assert dbfs_path_validate.call_count == 5
    assert put_file_mock.call_count == 4
    assert put_file_mock.call_args_list[0][0][0] == jar2
    assert put_file_mock.call_args_list[0][0][1].absolute_path ==\
        'dbfs:/pipelines/code/51eac6b471a284d3341d8c0c63d0f1a286262a18.jar'
    assert put_file_mock.call_args_list[1][0][0] == jar3_relpath
    assert put_file_mock.call_args_list[2][0][0] == jar4
    assert put_file_mock.call_args_list[3][0][0] == wheel1
    client_mock = pipelines_api.client.client.perform_query
    client_mock.assert_called_with('PUT', '/pipelines/' + PIPELINE_ID,
                                   data=expected_spec, headers=None)

    pipelines_api.deploy(spec, HEADERS)
    client_mock.assert_called_with('PUT', '/pipelines/' + PIPELINE_ID,
                                   data=expected_spec, headers=HEADERS)
    assert client_mock.call_count == 2


def test_delete(pipelines_api):
    pipelines_api.delete(PIPELINE_ID)
    client_mock = pipelines_api.client.client.perform_query
    assert client_mock.call_count == 1
    client_mock.assert_called_with('DELETE', '/pipelines/' + PIPELINE_ID,
                                   data=ID_ONLY_SPEC, headers=None)

    pipelines_api.delete(PIPELINE_ID, HEADERS)
    client_mock.assert_called_with('DELETE', '/pipelines/' + PIPELINE_ID,
                                   data=ID_ONLY_SPEC, headers=HEADERS)


def test_get(pipelines_api):
    response = pipelines_api.get(PIPELINE_ID)
    client_mock = pipelines_api.client.client.perform_query
    assert client_mock.call_count == 1
    client_mock.assert_called_with('GET', '/pipelines/' + PIPELINE_ID, data={}, headers=None)
    assert(response['pipeline_id'] == PIPELINE_ID and response['state'] == 'RUNNING')


def test_reset(pipelines_api):
    pipelines_api.reset(PIPELINE_ID)
    client_mock = pipelines_api.client.client.perform_query
    assert client_mock.call_count == 1
    client_mock.assert_called_with('POST', '/pipelines/{}/reset'.format(PIPELINE_ID),
                                   data=ID_ONLY_SPEC, headers=None)


def test_partition_local_remote(pipelines_api):
    libraries = [
        # local files
        LibraryObject('jar', '/absolute/path/abc.ext'),
        LibraryObject('jar', 'relative/path.ext'),
        LibraryObject('jar', 'file:///file/scheme/abs/path.ext'),
        LibraryObject('jar', 'FILE:/all/caps.ext'),
        LibraryObject('jar', 'FiLe:/weird/case.ext'),
        LibraryObject('jar', 'file.ext'),
        LibraryObject('whl', 'rel/path.ext'),
        # shouldn't be uploaded
        LibraryObject('jar', 'dbfs:/absolute/path/abc.ext'),
        LibraryObject('jar', 's3:file:/file/scheme/abs/path.ext'),
        LibraryObject('jar', 'scheme:file.ext'),
        LibraryObject('jar', 'scheme:/abs/path.ext'),
        LibraryObject('jar', 'scheme://abs/path.ext'),
        LibraryObject('egg', 'file:/abs/path.ext')
    ]
    expected_llo = [
        LibraryObject('jar', '/absolute/path/abc.ext'),
        LibraryObject('jar', 'relative/path.ext'),
        LibraryObject('jar', '/file/scheme/abs/path.ext'),
        LibraryObject('jar', '/all/caps.ext'),
        LibraryObject('jar', '/weird/case.ext'),
        LibraryObject('jar', 'file.ext'),
        LibraryObject('whl', 'rel/path.ext')
    ]
    expected_external = [
        LibraryObject('jar', 'dbfs:/absolute/path/abc.ext'),
        LibraryObject('jar', 's3:file:/file/scheme/abs/path.ext'),
        LibraryObject('jar', 'scheme:file.ext'),
        LibraryObject('jar', 'scheme:/abs/path.ext'),
        LibraryObject('jar', 'scheme://abs/path.ext'),
        LibraryObject('egg', 'file:/abs/path.ext')
    ]
    llo, external = pipelines_api._identify_local_libraries(libraries)
    assert llo == expected_llo
    assert external == expected_external


def test_library_object_serialization_deserialization():
    libraries = [
        {'jar': '/absolute/path/abc.ext'},
        {'jar': 'relative/path.ext'},
        {'jar': 'file:/file/scheme/relative/path.ext'},
        {'jar': 'FILE:/all/caps.ext'},
        {'egg': 'FiLe:/weird/case.ext'},
        {'whl': 'file.ext'},
        {'whl': 's3:/s3/path/file.ext'},
        {'jar': 'dbfs:/dbfs/path/file.ext'}
    ]
    library_objects = [
        LibraryObject('jar', '/absolute/path/abc.ext'),
        LibraryObject('jar', 'relative/path.ext'),
        LibraryObject('jar', 'file:/file/scheme/relative/path.ext'),
        LibraryObject('jar', 'FILE:/all/caps.ext'),
        LibraryObject('egg', 'FiLe:/weird/case.ext'),
        LibraryObject('whl', 'file.ext'),
        LibraryObject('whl', 's3:/s3/path/file.ext'),
        LibraryObject('jar', 'dbfs:/dbfs/path/file.ext')
    ]
    llo = LibraryObject.from_json(libraries)
    assert llo == library_objects

    libs = LibraryObject.to_json(library_objects)
    assert libs == libraries
