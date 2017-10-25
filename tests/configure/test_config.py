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

# pylint:disable=protected-access

import mock

import databricks_cli.configure.config as config


def test_require_config_valid():
    with mock.patch('databricks_cli.configure.config.DatabricksConfig') as DatabricksConfigMock:
        config_mock = DatabricksConfigMock.fetch_from_fs.return_value
        config_mock.is_valid = True

        @config.require_config
        def test_function(x):
            return x

        assert test_function(1) == 1


def test_require_config_invalid():
    with mock.patch('databricks_cli.configure.config.DatabricksConfig') as DatabricksConfigMock:
        with mock.patch('databricks_cli.configure.config.error_and_quit') as error_and_quit_mock:
            config_mock = DatabricksConfigMock.fetch_from_fs.return_value
            config_mock.is_valid = False

            @config.require_config
            def test_function(x):
                return x

            test_function(1)

            assert error_and_quit_mock.call_count == 1
            assert 'You haven\'t configured the CLI' in error_and_quit_mock.call_args[0][0]


class TestDatabricksConfig(object):
    databricks_config_from_password = config.DatabricksConfig.construct_from_password(
        'test-host', 'test-user', 'test-password')
    databricks_config_from_token = config.DatabricksConfig.construct_from_token(
        'test-host', 'test-token')

    def test_is_valid_true(self):
        assert self.databricks_config_from_password.is_valid
        assert self.databricks_config_from_token.is_valid

    def test_is_valid_false(self):
        databricks_config = config.DatabricksConfig()
        assert not databricks_config.is_valid

    def test_is_valid_with_password(self):
        assert self.databricks_config_from_password.is_valid_with_password
        assert not self.databricks_config_from_token.is_valid_with_password

    def test_is_valid_with_token(self):
        assert self.databricks_config_from_token.is_valid_with_token
        assert not self.databricks_config_from_password.is_valid_with_token

    def test_host(self):
        assert self.databricks_config_from_password.host == 'test-host'
        assert self.databricks_config_from_token.host == 'test-host'

    def test_username(self):
        assert self.databricks_config_from_password.username == 'test-user'
        assert self.databricks_config_from_token.username is None

    def test_password(self):
        assert self.databricks_config_from_password.password == 'test-password'
        assert self.databricks_config_from_token.password is None

    def test_token(self):
        assert self.databricks_config_from_password.token is None
        assert self.databricks_config_from_token.token == 'test-token'

    def test_fetch_from_fs(self, tmpdir):
        with mock.patch.object(config.DatabricksConfig, 'get_path') as get_path_mock:
            test_path = tmpdir.dirpath() + '/.databrickscfg'
            get_path_mock.return_value = str(test_path)

            self.databricks_config_from_token.overwrite()
            from_fs = config.DatabricksConfig.fetch_from_fs()
            assert self.databricks_config_from_token.token == from_fs.token
