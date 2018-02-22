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
from databricks_cli.configure.provider import DatabricksConfig


def test_require_config_valid():
    with mock.patch('databricks_cli.configure.config.get_config_for_profile') as \
            get_config_for_profile_mock:
        get_config_for_profile_mock.return_value = DatabricksConfig(
            'test-host', None, None, 'test-token')

        @config.require_config
        def test_function(x):
            return x

        assert test_function(1) == 1


def test_require_config_invalid():
    with mock.patch('databricks_cli.configure.config.error_and_quit') as error_and_quit_mock:
        with mock.patch('databricks_cli.configure.config.get_config_for_profile') as \
                get_config_for_profile_mock:
            get_config_for_profile_mock.return_value = DatabricksConfig(None, None, None, None)

            @config.require_config
            def test_function(x):
                return x

            test_function(1)

            assert error_and_quit_mock.call_count == 1
            assert 'You haven\'t configured the CLI' in error_and_quit_mock.call_args[0][0]
