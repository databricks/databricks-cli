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

import os
import pytest

from mock import patch
from databricks_cli.configure.provider import DatabricksConfig, DEFAULT_SECTION, \
    update_and_persist_config, get_config_for_profile, get_config, \
    set_config_provider, ProfileConfigProvider, _get_path, DatabricksConfigProvider
from databricks_cli.utils import InvalidConfigurationError


TEST_HOST = 'https://test.cloud.databricks.com'
TEST_USER = 'monkey@databricks.com'
TEST_PASSWORD = 'banana' # NOQA
TEST_TOKEN = 'dapiTESTING'
TEST_PROFILE = 'testing-profile'


def test_update_and_persist_config():
    config = DatabricksConfig.from_token(TEST_HOST, TEST_TOKEN)
    update_and_persist_config(DEFAULT_SECTION, config)
    config = get_config_for_profile(DEFAULT_SECTION)
    assert config.is_valid_with_token
    assert config.host == TEST_HOST
    assert config.token == TEST_TOKEN

    # Overwrite conf for same section.
    config = DatabricksConfig.from_password(TEST_HOST, TEST_USER, TEST_PASSWORD)
    update_and_persist_config(DEFAULT_SECTION, config)
    config = get_config_for_profile(DEFAULT_SECTION)
    assert config.is_valid_with_password
    assert not config.is_valid_with_token
    assert config.host == TEST_HOST
    assert config.username == TEST_USER
    assert config.password == TEST_PASSWORD


def test_update_and_persist_config_two_sections():
    config = DatabricksConfig.from_token(TEST_HOST, TEST_TOKEN)
    update_and_persist_config(DEFAULT_SECTION, config)

    # Overwrite conf for same section.
    config = DatabricksConfig.from_password(TEST_HOST, TEST_USER, TEST_PASSWORD)
    update_and_persist_config(TEST_PROFILE, config)

    config = get_config_for_profile(DEFAULT_SECTION)
    assert config.is_valid_with_token
    assert config.host == TEST_HOST
    assert config.token == TEST_TOKEN

    config = get_config_for_profile(TEST_PROFILE)
    assert config.is_valid_with_password
    assert config.host == TEST_HOST
    assert config.username == TEST_USER
    assert config.password == TEST_PASSWORD
    assert config.token is None


def test_update_and_persist_config_case_sensitive():
    config = DatabricksConfig.from_token(TEST_HOST, TEST_TOKEN)
    update_and_persist_config(TEST_PROFILE, config)

    config_2 = DatabricksConfig.from_password(TEST_HOST, TEST_USER, TEST_PASSWORD)
    update_and_persist_config(TEST_PROFILE.upper(), config_2)

    config = get_config_for_profile(TEST_PROFILE)
    assert config.is_valid_with_token
    assert not config.is_valid_with_password

    config_2 = get_config_for_profile(TEST_PROFILE.upper())
    assert config_2.is_valid_with_password
    assert not config_2.is_valid_with_token


def test_get_config_for_profile_empty():
    config = get_config_for_profile(TEST_PROFILE)
    assert not config.is_valid_with_password
    assert not config.is_valid_with_token
    assert config.host is None
    assert config.username is None
    assert config.password is None
    assert config.token is None


def test_get_config_if_token_environment_set():

    with patch.dict('os.environ', {'DATABRICKS_HOST': TEST_HOST,
                                   'DATABRICKS_TOKEN': TEST_TOKEN}):
        config = get_config_for_profile(TEST_PROFILE)
        assert config.host == TEST_HOST
        assert config.token == TEST_TOKEN


def test_get_config_if_password_environment_set():
    with patch.dict('os.environ', {'DATABRICKS_HOST': TEST_HOST,
                                   'DATABRICKS_USERNAME': TEST_USER,
                                   'DATABRICKS_PASSWORD': TEST_PASSWORD}):
        config = get_config_for_profile(TEST_PROFILE)
        assert config.host == TEST_HOST
        assert config.username == TEST_USER
        assert config.password == TEST_PASSWORD


def test_get_config_uses_default_profile():
    config = DatabricksConfig.from_token("hosty", "hello")
    update_and_persist_config(DEFAULT_SECTION, config)
    config = get_config()
    assert config.is_valid_with_token
    assert config.host == "hosty"
    assert config.token == "hello"


def test_get_config_uses_env_variable():
    with patch.dict('os.environ', {'DATABRICKS_HOST': TEST_HOST,
                                   'DATABRICKS_USERNAME': TEST_USER,
                                   'DATABRICKS_PASSWORD': TEST_PASSWORD}):
        config = get_config()
        assert config.host == TEST_HOST
        assert config.username == TEST_USER
        assert config.password == TEST_PASSWORD


def test_get_config_throw_exception_if_profile_invalid():
    invalid_config = DatabricksConfig.from_token(None, None)
    update_and_persist_config(DEFAULT_SECTION, invalid_config)
    with pytest.raises(InvalidConfigurationError):
        get_config()


def test_get_config_throw_exception_if_profile_absent():
    assert not os.path.exists(_get_path())
    with pytest.raises(InvalidConfigurationError):
        get_config()


def test_get_config_override_profile():
    config = DatabricksConfig.from_token("yo", "lo")
    update_and_persist_config(TEST_PROFILE, config)
    try:
        provider = ProfileConfigProvider(TEST_PROFILE)
        set_config_provider(provider)
        config = get_config()
        assert config.host == "yo"
        assert config.token == "lo"
    finally:
        set_config_provider(None)


def test_get_config_override_custom():
    class TestConfigProvider(DatabricksConfigProvider):
        def get_config(self):
            return DatabricksConfig.from_token("Override", "Token!")

    try:
        provider = TestConfigProvider()
        set_config_provider(provider)
        config = get_config()
        assert config.host == "Override"
        assert config.token == "Token!"
    finally:
        set_config_provider(None)


def test_get_config_bad_override():
    with pytest.raises(Exception):
        set_config_provider("NotAConfigProvider")
