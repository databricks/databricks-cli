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
    def test_is_valid_true(self):
        databricks_config = config.DatabricksConfig('host', 'username', 'password')
        assert databricks_config.is_valid

    def test_is_valid_false(self):
        databricks_config = config.DatabricksConfig('host', 'username')
        assert not databricks_config.is_valid
