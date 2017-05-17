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
from databricks_cli.dbfs.dbfs_path import DbfsPath

TEST_DBFS_PATH = DbfsPath('dbfs:/test')


class TestDbfsPath(object):
    def test_from_api_path_valid(self):
        dbfs_path = DbfsPath.from_api_path('/test')
        assert dbfs_path == TEST_DBFS_PATH

    def test_from_api_path_invalid(self):
        with mock.patch('databricks_cli.dbfs.dbfs_path.error_and_quit') as error_and_quit_mock:
            DbfsPath.from_api_path('dbfs:/test')
            assert error_and_quit_mock.call_count == 1
            assert 'must start with' in error_and_quit_mock.call_args[0][0]

    def test_is_valid_true(self):
        assert DbfsPath.is_valid('dbfs:/test')

    def test_is_valid_false(self):
        assert not DbfsPath.is_valid('/test')
        assert not DbfsPath.is_valid('test')

    def test_join(self):
        assert DbfsPath('dbfs:/test/a') == TEST_DBFS_PATH.join('a')

    def test_relpath(self):
        assert DbfsPath('dbfs:/test/a').relpath(TEST_DBFS_PATH) == 'a'

    def test_basename(self):
        assert DbfsPath('dbfs:/').basename == ''
        assert DbfsPath('dbfs:/test').basename == 'test'
        assert DbfsPath('dbfs:/test/').basename == 'test'

    def test_is_absolute_path(self):
        assert DbfsPath('dbfs:/').is_absolute_path
        assert not DbfsPath('test', validate=False).is_absolute_path

    def test_is_root(self):
        assert DbfsPath('dbfs:/').is_root
        assert not DbfsPath('test', validate=False).is_root

    def test_eq(self):
        assert DbfsPath('dbfs:/') == DbfsPath('dbfs:/')
        assert DbfsPath('dbfs:/') != 'bad type'
