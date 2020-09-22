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
from databricks_cli.dbfs.exceptions import LocalFileExistsException, RateLimitException

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


def get_partial_delete_exception(message="[...] operation has deleted 10 files [...]"):
    response = requests.Response()
    response.status_code = 503
    response._content = ('{{"error_code": "{}","message": "{}"}}'.format(api.DbfsErrorCodes.PARTIAL_DELETE, message)).encode() #  NOQA
    return requests.exceptions.HTTPError(response=response)


def get_rate_limit_exception():
    response = requests.Response()
    response.status_code = 429
    response._content = ('{{"error_code": "{}"}}'.format(api.DbfsErrorCodes.TOO_MANY_REQUESTS)).encode() #  NOQA
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

    def test_list_files_rate_limited(self, dbfs_api):
        rate_limit_exception = get_rate_limit_exception()
        # Simulate 2 rate limit exceptions followed by a full successful operation
        exception_sequence = [rate_limit_exception, rate_limit_exception, None]
        dbfs_api.client.list_files = mock.Mock(side_effect=exception_sequence)
        # Should succeed
        files = dbfs_api.list_files(TEST_DBFS_PATH)

        assert len(files) == 0

    def test_mkdirs_rate_limited(self, dbfs_api):
        rate_limit_exception = get_rate_limit_exception()
        # Simulate 2 rate limit exceptions followed by a full successful operation
        exception_sequence = [rate_limit_exception, rate_limit_exception, None]
        dbfs_api.client.mkdirs = mock.Mock(side_effect=exception_sequence)
        # Should succeed
        dbfs_api.mkdirs(DbfsPath('dbfs:/test/mkdir'))
        files = dbfs_api.client.list(DbfsPath('dbfs:/test/mkdir'))

        assert len(files) == 0

    def test_mkdirs_stop_retrying(self, dbfs_api):
        rate_limit_exception = get_rate_limit_exception()
        # Simulate 9 rate limit exceptions which will fail eventually
        exception_sequence = [rate_limit_exception, rate_limit_exception, rate_limit_exception, 
                              rate_limit_exception, rate_limit_exception, rate_limit_exception, 
                              rate_limit_exception, rate_limit_exception, rate_limit_exception]
        dbfs_api.client.mkdirs = mock.Mock(side_effect=exception_sequence)
        with pytest.raises(RateLimitException):
            dbfs_api.mkdirs(DbfsPath('dbfs:/test/mkdir'))
        assert dbfs_api.client.mkdirs.call_count == 7

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

    def test_get_status_rate_limited(self, dbfs_api):
        exception = get_rate_limit_exception()
        dbfs_api.client.get_status = mock.Mock(side_effect=[exception, {
            'path': '/test',
            'is_dir': False,
            'file_size': 1
        }])
        # Should succeed
        assert dbfs_api.get_status(TEST_DBFS_PATH) is not None

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
        assert api_mock.close.call_count == 1
        assert test_handle == api_mock.close.call_args[0][0]

    def test_put_file_rate_limited(self, dbfs_api, tmpdir):
        test_file_path = os.path.join(tmpdir.strpath, 'test')
        with open(test_file_path, 'wt') as f:
            f.write('test')

        exception = get_rate_limit_exception()
        api_mock = dbfs_api.client
        test_handle = 0
        api_mock.create = mock.Mock(side_effect=[exception, {'handle': test_handle}])
        api_mock.add_block = mock.Mock(side_effect=[exception, None])
        api_mock.close = mock.Mock(side_effect=[exception, None])
        dbfs_api.put_file(test_file_path, TEST_DBFS_PATH, True)

        assert api_mock.create.call_count == 2
        assert api_mock.add_block.call_count == 2
        assert test_handle == api_mock.add_block.call_args[0][0]
        assert b64encode(b'test').decode() == api_mock.add_block.call_args[0][1]
        assert api_mock.close.call_count == 2
        assert test_handle == api_mock.close.call_args[0][0]

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

    def test_get_file_rate_limited(self, dbfs_api, tmpdir):
        api_mock = dbfs_api.client
        api_mock.get_status.return_value = TEST_FILE_JSON
        rate_limit_exception = get_rate_limit_exception()

        api_mock.read = mock.Mock(side_effect=[rate_limit_exception, {
            'bytes_read': 1,
            'data': b64encode(b'x'),
        }])

        test_file_path = os.path.join(tmpdir.strpath, 'test')
        dbfs_api.get_file(TEST_DBFS_PATH, test_file_path, True)

        with open(test_file_path, 'r') as f:
            assert f.read() == 'x'

    def test_cat(self, dbfs_api):
        dbfs_api.client.get_status.return_value = {
            'path': '/test',
            'is_dir': False,
            'file_size': 1
        }
        dbfs_api.client.read.return_value = {
            'bytes_read': 1,
            'data': b64encode(b'a'),
        }
        with mock.patch('databricks_cli.dbfs.api.click') as click_mock:
            dbfs_api.cat('dbfs:/whatever-doesnt-matter')
            click_mock.echo.assert_called_with('a', nl=False)

    def test_cat_rate_limited(self, dbfs_api):
        rate_limit_exception = get_rate_limit_exception()
        # Simulate 2 rate limit exceptions followed by a full successful operation
        get_status_exception_sequence = [rate_limit_exception, rate_limit_exception, {
            'path': '/test',
            'is_dir': False,
            'file_size': 1
        }]
        read_exception_sequence = [rate_limit_exception, rate_limit_exception, {
            'bytes_read': 1,
            'data': b64encode(b'a'),
        }]

        dbfs_api.client.get_status = mock.Mock(side_effect=get_status_exception_sequence)
        dbfs_api.client.read = mock.Mock(side_effect=read_exception_sequence)

        with mock.patch('databricks_cli.dbfs.api.click') as click_mock:
            dbfs_api.cat('dbfs:/whatever-doesnt-matter')
            click_mock.echo.assert_called_with('a', nl=False)

    def test_partial_delete(self, dbfs_api):
        e_partial_delete = get_partial_delete_exception()
        # Simulate 3 partial deletes followed by a full successful operation
        exception_sequence = [e_partial_delete, e_partial_delete, e_partial_delete, None]
        dbfs_api.client.delete = mock.Mock(side_effect=exception_sequence)
        dbfs_api.delete_retry_delay_millis = 1
        # Should succeed
        dbfs_api.delete(DbfsPath('dbfs:/whatever-doesnt-matter'), recursive=True)

    def test_partial_delete_with_rate_limit(self, dbfs_api):
        rate_limit_exception = get_rate_limit_exception()
        e_partial_delete = get_partial_delete_exception()
        # Simulate 3 partial deletes interspersed with rate limiting exceptions 
        # followed by a full successful delete
        exception_sequence = [e_partial_delete, rate_limit_exception, 
                              e_partial_delete, rate_limit_exception,
                              e_partial_delete, None]

        dbfs_api.client.delete = mock.Mock(side_effect=exception_sequence)
        dbfs_api.delete_retry_delay_millis = 1
        # Should succeed
        dbfs_api.delete(DbfsPath('dbfs:/whatever-doesnt-matter'), recursive=True)

    def test_delete_with_rate_limit(self, dbfs_api):
        rate_limit_exception = get_rate_limit_exception()
        # Simulate a rate limit exception followed by a full successful delete
        exception_sequence = [rate_limit_exception, None]
        dbfs_api.client.delete = mock.Mock(side_effect=exception_sequence)
        dbfs_api.delete_retry_delay_millis = 1
        # Should succeed
        dbfs_api.delete(DbfsPath('dbfs:/whatever-doesnt-matter'), recursive=True)

    def test_partial_delete_exception_message_parse_error(self, dbfs_api):
        message = "unexpected partial delete exception message"
        e_partial_delete = get_partial_delete_exception(message)
        dbfs_api.client.delete = mock.Mock(side_effect=[e_partial_delete, None])
        dbfs_api.delete_retry_delay_millis = 1
        # Should succeed
        dbfs_api.delete(DbfsPath('dbfs:/whatever-doesnt-matter'), recursive=True)

    def test_get_num_files_deleted(self):
        e_partial_delete = get_partial_delete_exception()
        # Should succeed
        api.DbfsApi.get_num_files_deleted(e_partial_delete)

    def test_get_num_files_deleted_parse_error(self):
        message = "unexpected partial delete exception message"
        e_partial_delete = get_partial_delete_exception(message)
        # Should raise api.ParseException
        with pytest.raises(api.ParseException):
            api.DbfsApi.get_num_files_deleted(e_partial_delete)
