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
from base64 import b64encode

import os
import requests
import mock
import pytest

import databricks_cli.dbfs.api as api
from databricks_cli.dbfs.dbfs_path import DbfsPath
from databricks_cli.dbfs.exceptions import LocalFileExistsException

TEST_DBFS_PATH = DbfsPath('dbfs:/test')
TEST_FILE_JSON = {
    'path': '/test',
    'is_dir': False,
    'file_size': 1
}
TEST_FILE_INFO = api.FileInfo(TEST_DBFS_PATH, False, 1)


def get_resource_does_not_exist_exception():
    response = requests.Response()
    response._content = ('{"error_code": "' + api.DbfsErrorCodes.RESOURCE_DOES_NOT_EXIST + '"}').encode() #  NOQA
    return requests.exceptions.HTTPError(response=response)


class TestFileInfo(object):
    def test_to_row_not_long_form_not_absolute(self):
        file_info = api.FileInfo(TEST_DBFS_PATH, False, 1)
        row = file_info.to_row(is_long_form=False, is_absolute=False)
        assert len(row) == 1
        assert TEST_DBFS_PATH.basename == row[0]

    def test_to_row_long_form_not_absolute(self):
        file_info = api.FileInfo(TEST_DBFS_PATH, False, 1)
        row = file_info.to_row(is_long_form=True, is_absolute=False)
        assert len(row) == 3
        assert row[0] == 'file'
        assert row[1] == 1
        assert TEST_DBFS_PATH.basename == row[2]

    def test_from_json(self):
        file_info = api.FileInfo.from_json(TEST_FILE_JSON)
        assert file_info.dbfs_path == TEST_DBFS_PATH
        assert not file_info.is_dir
        assert file_info.file_size == 1


@pytest.fixture()
def dbfs_api():
    with mock.patch('databricks_cli.dbfs.api.DbfsService') as DbfsServiceMock:
        DbfsServiceMock.return_value = mock.MagicMock()
        _dbfs_api = api.DbfsApi(None)
        yield _dbfs_api


class TestDbfsApi(object):
    def test_list_files_exists(self, dbfs_api):
        json = {
            'files': [TEST_FILE_JSON]
        }
        dbfs_api.client.list.return_value = json
        files = dbfs_api.list_files(TEST_DBFS_PATH)

        assert len(files) == 1
        assert TEST_FILE_INFO == files[0]

    def test_list_files_does_not_exist(self, dbfs_api):
        json = {}
        dbfs_api.client.list.return_value = json
        files = dbfs_api.list_files(TEST_DBFS_PATH)

        assert len(files) == 0

    def test_file_exists_true(self, dbfs_api):
        dbfs_api.client.get_status.return_value = TEST_FILE_JSON
        assert dbfs_api.file_exists(TEST_DBFS_PATH)

    def test_file_exists_false(self, dbfs_api):
        exception = get_resource_does_not_exist_exception()
        dbfs_api.client.get_status = mock.Mock(side_effect=exception)
        assert not dbfs_api.file_exists(TEST_DBFS_PATH)

    def test_get_status(self, dbfs_api):
        dbfs_api.client.get_status.return_value = TEST_FILE_JSON
        assert dbfs_api.get_status(TEST_DBFS_PATH) == TEST_FILE_INFO

    def test_get_status_fail(self, dbfs_api):
        exception = get_resource_does_not_exist_exception()
        dbfs_api.client.get_status = mock.Mock(side_effect=exception)
        with pytest.raises(exception.__class__):
            dbfs_api.get_status(TEST_DBFS_PATH)

    def test_put_file(self, dbfs_api, tmpdir):
        test_file_path = os.path.join(tmpdir.strpath, 'test')
        with open(test_file_path, 'wt') as f:
            f.write('test')

        api_mock = dbfs_api.client
        test_handle = 0
        api_mock.create.return_value = {'handle': test_handle}
        dbfs_api.put_file(test_file_path, TEST_DBFS_PATH, True)

        assert api_mock.add_block.call_count == 1
        assert test_handle == api_mock.add_block.call_args[0][0]
        assert b64encode(b'test').decode() == api_mock.add_block.call_args[0][1]

    def test_get_file_check_overwrite(self, dbfs_api, tmpdir):
        test_file_path = os.path.join(tmpdir.strpath, 'test')
        with open(test_file_path, 'w') as f:
            f.write('test')
        with pytest.raises(LocalFileExistsException):
            dbfs_api.get_file(TEST_DBFS_PATH, test_file_path, False)

    def test_get_file(self, dbfs_api, tmpdir):
        api_mock = dbfs_api.client
        api_mock.get_status.return_value = TEST_FILE_JSON
        api_mock.read.return_value = {
            'bytes_read': 1,
            'data': b64encode(b'x'),
        }

        test_file_path = os.path.join(tmpdir.strpath, 'test')
        dbfs_api.get_file(TEST_DBFS_PATH, test_file_path, True)

        with open(test_file_path, 'r') as f:
            assert f.read() == 'x'
