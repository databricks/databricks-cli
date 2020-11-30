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
# pylint: disable-msg=too-many-locals


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
SPEC_WITHOUT_ID = {k: v for k, v in SPEC.items() if k != "id"}
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
def test_create_pipeline_and_upload_libraries(put_file_mock, dbfs_path_validate, pipelines_api,
                                              tmpdir):
    _test_library_uploads(pipelines_api, pipelines_api.create, SPEC_WITHOUT_ID, put_file_mock,
                          dbfs_path_validate, tmpdir, False)


@mock.patch('databricks_cli.dbfs.api.DbfsApi.file_exists', file_exists_stub)
@mock.patch('databricks_cli.dbfs.dbfs_path.DbfsPath.validate')
@mock.patch('databricks_cli.dbfs.api.DbfsApi.put_file')
def test_deploy_pipeline_and_upload_libraries(put_file_mock, dbfs_path_validate, pipelines_api,
                                              tmpdir):
    _test_library_uploads(pipelines_api, pipelines_api.deploy, SPEC, put_file_mock,
                          dbfs_path_validate, tmpdir, False)


def _test_library_uploads(pipelines_api, api_method, spec, put_file_mock, dbfs_path_validate,
                          tmpdir, allow_duplicate_names):
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
    spec = copy.deepcopy(spec)

    # set-up the test
    jar1 = tmpdir.join('jar1.jar').strpath
    jar2 = tmpdir.join('jar2.jar').strpath
    jar3 = tmpdir.join('jar3.jar').strpath
    jar4 = tmpdir.join('jar4.jar').strpath
    wheel1 = tmpdir.join('wheel-name-conv.whl').strpath
    jar3_relpath = os.path.relpath(jar3, os.getcwd())
    jar4_file_prefix = 'file:{}'.format(jar4)
    remote_path_456 = 'dbfs:/pipelines/code/51eac6b471a284d3341d8c0c63d0f1a286262a18.jar'

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
                 {'maven': {'coordinates': 'com.org.name:package:0.1.0'}},
                 {'jar': jar1},
                 {'jar': jar2},
                 {'jar': jar3_relpath},
                 {'jar': jar4_file_prefix},
                 {'whl': wheel1}]

    expected_data = copy.deepcopy(spec)

    spec['libraries'] = libraries

    expected_data['libraries'] = [
        {'jar': 'dbfs:/pipelines/code/file.jar'},
        {'maven': {'coordinates': 'com.org.name:package:0.1.0'}},
        {'jar': 'dbfs:/pipelines/code/40bd001563085fc35165329ea1ff5c5ecbdbbeef.jar'},
        {'jar': 'dbfs:/pipelines/code/51eac6b471a284d3341d8c0c63d0f1a286262a18.jar'},
        {'jar': 'dbfs:/pipelines/code/51eac6b471a284d3341d8c0c63d0f1a286262a18.jar'},
        {'jar': 'dbfs:/pipelines/code/51eac6b471a284d3341d8c0c63d0f1a286262a18.jar'},
        {'whl': 'dbfs:/pipelines/code/51eac6b471a284d3341d8c0c63d0f1a286262a18/wheel-name-conv.whl'}
    ]
    expected_data['allow_duplicate_names'] = allow_duplicate_names

    api_method(spec, allow_duplicate_names)
    assert dbfs_path_validate.call_count == 5
    assert put_file_mock.call_count == 4
    assert put_file_mock.call_args_list[0][0][0] == jar2
    assert put_file_mock.call_args_list[0][0][1].absolute_path == remote_path_456
    assert put_file_mock.call_args_list[1][0][0] == jar3_relpath
    assert put_file_mock.call_args_list[2][0][0] == jar4
    assert put_file_mock.call_args_list[3][0][0] == wheel1
    client_mock = pipelines_api.client.client.perform_query
    assert client_mock.call_count == 1
    assert client_mock.call_args_list[0][1]['data'] == expected_data


def test_create(pipelines_api):
    client_mock = pipelines_api.client.client.perform_query

    spec = copy.deepcopy(SPEC_WITHOUT_ID)
    spec['libraries'] = []

    pipelines_api.create(spec, allow_duplicate_names=False)
    data = copy.deepcopy(spec)
    data['allow_duplicate_names'] = False
    client_mock.assert_called_with("POST", "/pipelines", data=data, headers=None)
    assert client_mock.call_count == 1

    pipelines_api.create(spec, allow_duplicate_names=True, headers=HEADERS)
    data = copy.deepcopy(spec)
    data['allow_duplicate_names'] = True
    client_mock.assert_called_with("POST", "/pipelines", data=data, headers=HEADERS)
    assert client_mock.call_count == 2


def test_deploy(pipelines_api):
    client_mock = pipelines_api.client.client.perform_query

    spec = copy.deepcopy(SPEC)
    spec['libraries'] = []

    pipelines_api.deploy(spec, allow_duplicate_names=False)
    data = copy.deepcopy(spec)
    data['allow_duplicate_names'] = False
    client_mock.assert_called_with("PUT", "/pipelines/" + PIPELINE_ID, data=data, headers=None)
    assert client_mock.call_count == 1

    pipelines_api.deploy(spec, allow_duplicate_names=True, headers=HEADERS)
    data = copy.deepcopy(spec)
    data['allow_duplicate_names'] = True
    client_mock.assert_called_with("PUT", "/pipelines/" + PIPELINE_ID, data=data, headers=HEADERS)
    assert client_mock.call_count == 2


def test_delete(pipelines_api):
    pipelines_api.delete(PIPELINE_ID)
    client_mock = pipelines_api.client.client.perform_query
    assert client_mock.call_count == 1
    client_mock.assert_called_with('DELETE', '/pipelines/' + PIPELINE_ID,
                                   data={}, headers=None)

    pipelines_api.delete(PIPELINE_ID, HEADERS)
    client_mock.assert_called_with('DELETE', '/pipelines/' + PIPELINE_ID,
                                   data={}, headers=HEADERS)


def test_get(pipelines_api):
    response = pipelines_api.get(PIPELINE_ID)
    client_mock = pipelines_api.client.client.perform_query
    assert client_mock.call_count == 1
    client_mock.assert_called_with('GET', '/pipelines/' + PIPELINE_ID, data={}, headers=None)
    assert (response['pipeline_id'] == PIPELINE_ID and response['state'] == 'RUNNING')


def test_reset(pipelines_api):
    pipelines_api.reset(PIPELINE_ID)
    client_mock = pipelines_api.client.client.perform_query
    assert client_mock.call_count == 1
    client_mock.assert_called_with('POST', '/pipelines/{}/reset'.format(PIPELINE_ID),
                                   data={}, headers=None)


def test_run(pipelines_api):
    pipelines_api.run(PIPELINE_ID)
    client_mock = pipelines_api.client.client.perform_query
    assert client_mock.call_count == 1
    client_mock.assert_called_with('POST', '/pipelines/{}/run'.format(PIPELINE_ID),
                                   data={}, headers=None)


def test_stop(pipelines_api):
    pipelines_api.stop(PIPELINE_ID)
    client_mock = pipelines_api.client.client.perform_query
    assert client_mock.call_count == 1
    client_mock.assert_called_with('POST', '/pipelines/{}/stop'.format(PIPELINE_ID),
                                   data={}, headers=None)


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
        LibraryObject('egg', 'file:/abs/path.ext'),
        LibraryObject('maven', {'coordinates': 'com.org.name:package:0.1.0'})
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
        LibraryObject('egg', 'file:/abs/path.ext'),
        LibraryObject('maven', {'coordinates': 'com.org.name:package:0.1.0'})
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
        {'jar': 'dbfs:/dbfs/path/file.ext'},
        {'maven': {'coordinates': 'com.org.name:package:0.1.0'}},
        {'maven': {
            'coordinates': 'com.org.name:package:0.1.0',
            'exclusions': ['slf4j:slf4j', '*:hadoop-client']}}
    ]
    library_objects = [
        LibraryObject('jar', '/absolute/path/abc.ext'),
        LibraryObject('jar', 'relative/path.ext'),
        LibraryObject('jar', 'file:/file/scheme/relative/path.ext'),
        LibraryObject('jar', 'FILE:/all/caps.ext'),
        LibraryObject('egg', 'FiLe:/weird/case.ext'),
        LibraryObject('whl', 'file.ext'),
        LibraryObject('whl', 's3:/s3/path/file.ext'),
        LibraryObject('jar', 'dbfs:/dbfs/path/file.ext'),
        LibraryObject('maven', {'coordinates': 'com.org.name:package:0.1.0'}),
        LibraryObject('maven', {
            'coordinates': 'com.org.name:package:0.1.0',
            'exclusions': ['slf4j:slf4j', '*:hadoop-client']})
    ]
    llo = LibraryObject.from_json(libraries)
    assert llo == library_objects

    libs = LibraryObject.to_json(library_objects)
    assert libs == libraries


def test_list(pipelines_api):
    client_mock = pipelines_api.client.client.perform_query
    client_mock.side_effect = [{"statuses": [], "pagination": {}}]

    pipelines_api.list()
    assert client_mock.call_count == 1
    client_mock.assert_called_with('GET', '/pipelines',
                                   data={},
                                   headers=None)


def test_list_with_page_token(pipelines_api):
    client_mock = pipelines_api.client.client.perform_query
    client_mock.side_effect = [{"statuses": [], "pagination": {"next_page_token": "a"}},
                               {"statuses": [], "pagination": {}}]

    pipelines_api.list()
    assert client_mock.call_count == 2
    client_mock.assert_called_with('GET', '/pipelines',
                                   data={"pagination.page_token": "a"}, headers=None)


def test_list_with_paginated_responses(pipelines_api):
    client_mock = pipelines_api.client.client.perform_query
    client_mock.side_effect = [
        {'statuses': [{'pipeline_id': '1',
                       'state': 'RUNNING',
                       'cluster_id': '1024-161828-gram477',
                       'name': 'windfarm-pipe-v2',
                       'health': 'HEALTHY'},
                      {'pipeline_id': '2',
                       'state': 'RUNNING',
                       'cluster_id': '1024-160918-tees475',
                       'name': 'Wiki Pipeline',
                       'health': 'HEALTHY'}],
         'pagination': {'next_page_token': 'page2'}
         },
        {'statuses': [{'pipeline_id': '3',
                       'state': 'RUNNING',
                       'cluster_id': '1026-062128-blare168',
                       'name': 'airline-demo-workflow',
                       'health': 'HEALTHY'},
                      {'pipeline_id': '4',
                       'state': 'RUNNING',
                       'cluster_id': '1023-093505-corm4',
                       'name': 'Jira Automation Staging',
                       'health': 'HEALTHY'}],
         'pagination': {
             'next_page_token': 'page3',
             'prev_page_token': 'page2'}
         },
        {'statuses': [{'pipeline_id': '5',
                       'state': 'FAILED',
                       'cluster_id': '1023-090246-helix16',
                       'name': 'Marek',
                       'health': 'UNHEALTHY'},
                      {'pipeline_id': '6',
                       'state': 'RUNNING',
                       'cluster_id': '1027-061023-clasp844',
                       'name': 'Pipeline Demo NYCTaxi',
                       'health': 'HEALTHY'}],
         'pagination': {
             'prev_page_token': 'page3'}
         }
    ]

    pipelines = pipelines_api.list()

    assert client_mock.call_count == 3
    client_mock.assert_has_calls(
        [
            mock.call('GET', '/pipelines',
                      data={},
                      headers=None),
            mock.call('GET', '/pipelines',
                      data={"pagination.page_token": "page2"},
                      headers=None),
            mock.call('GET', '/pipelines',
                      data={"pagination.page_token": "page3"},
                      headers=None)
        ], any_order=False)

    assert [status["pipeline_id"] for status in pipelines] == ["1", "2", "3", "4", "5", "6"]


def test_list_with_no_returned_pipelines(pipelines_api):
    client_mock = pipelines_api.client.client.perform_query
    client_mock.side_effect = [
        {'statuses': [{'pipeline_id': '1',
                       'state': 'RUNNING',
                       'cluster_id': '1024-161828-gram477',
                       'name': 'windfarm-pipe-v2',
                       'health': 'HEALTHY'},
                      {'pipeline_id': '2',
                       'state': 'RUNNING',
                       'cluster_id': '1024-160918-tees475',
                       'name': 'Wiki Pipeline',
                       'health': 'HEALTHY'}],
         'pagination': {'next_page_token': 'page2'}
         },
        {}
    ]

    pipelines = pipelines_api.list()

    assert client_mock.call_count == 2
    client_mock.assert_has_calls(
        [
            mock.call('GET', '/pipelines',
                      data={},
                      headers=None),
            mock.call('GET', '/pipelines',
                      data={"pagination.page_token": "page2"},
                      headers=None)
        ], any_order=False)

    assert [status["pipeline_id"] for status in pipelines] == ["1", "2"]


def test_list_without_pagination(pipelines_api):
    client_mock = pipelines_api.client.client.perform_query
    client_mock.side_effect = [
        {'statuses': [{'pipeline_id': '1',
                       'state': 'RUNNING',
                       'cluster_id': '1024-161828-gram477',
                       'name': 'windfarm-pipe-v2',
                       'health': 'HEALTHY'}],
         }
    ]

    pipelines = pipelines_api.list()

    assert client_mock.call_count == 1
    client_mock.assert_has_calls(
        [
            mock.call('GET', '/pipelines',
                      data={},
                      headers=None)
        ], any_order=False)

    assert [status["pipeline_id"] for status in pipelines] == ["1"]
