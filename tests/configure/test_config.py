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
import pytest

import databricks_cli.configure.config as config
from databricks_cli.configure.provider import DatabricksConfig, DEFAULT_SECTION
from databricks_cli.utils import InvalidConfigurationError


def test_require_config_valid():
    with mock.patch('databricks_cli.configure.config.get_config_for_profile') as \
            get_config_for_profile_mock:
        get_config_for_profile_mock.return_value = DatabricksConfig(
            'test-host', None, None, 'test-token')

        @config.provide_api_client
        def test_function(api_client, x): # noqa
            return x

        assert test_function(x=1, profile=DEFAULT_SECTION) == 1 # noqa


def test_require_config_invalid():
    with mock.patch('databricks_cli.configure.config.get_config_for_profile') as \
            get_config_for_profile_mock:
        with mock.patch('databricks_cli.configure.config._get_api_client') as \
                get_api_client_mock: # noqa
            get_config_for_profile_mock.return_value = DatabricksConfig(None, None, None, None)

            @config.provide_api_client
            def test_function(api_client, x): # noqa
                return x

            with pytest.raises(InvalidConfigurationError):
                test_function(x=1, profile=DEFAULT_SECTION) # noqa
