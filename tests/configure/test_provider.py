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

from configparser import ConfigParser
from mock import patch
import pytest

from databricks_cli.configure.provider import DatabricksConfig, DEFAULT_SECTION, \
    update_and_persist_config, get_config_for_profile, get_config, \
    set_config_provider, ProfileConfigProvider, _get_path, DatabricksConfigProvider,\
    SparkTaskContextConfigProvider, _overwrite_config
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


def test_get_config_uses_task_context_variable():
    class TaskContextMock(object):

        def __init__(self):
            pass

        def getLocalProperty(self, x):  # NOQA
            if x == "spark.databricks.api.url":
                return "url"
            elif x == "spark.databricks.token":
                return "token"
            elif x == "spark.databricks.ignoreTls":
                return "True"
            else:
                raise Exception("should not get here.")

    ctx_class = ("databricks_cli.configure.provider.SparkTaskContextConfigProvider."
                 "_get_spark_task_context_or_none")
    with patch(ctx_class) as get_context_mock:
        get_context_mock.return_value = TaskContextMock()
        config = get_config()
        assert config.host == "url"
        assert config.token == "token"
        assert config.insecure == "True"
        assert config.username is None
        assert config.password is None


def test_task_context_provider_does_not_break_stuff():
    assert SparkTaskContextConfigProvider().get_config() is None


def test_get_config_uses_env_variable():
    with patch.dict('os.environ', {'DATABRICKS_HOST': TEST_HOST,
                                   'DATABRICKS_USERNAME': TEST_USER,
                                   'DATABRICKS_PASSWORD': TEST_PASSWORD}):
        config = get_config()
        assert config.host == TEST_HOST
        assert config.username == TEST_USER
        assert config.password == TEST_PASSWORD


def test_get_config_uses_path_env_variable(tmpdir):
    cfg_file = tmpdir.join("some-cfg-path").strpath
    with patch.dict('os.environ', {'DATABRICKS_CONFIG_FILE': cfg_file}):
        config = DatabricksConfig.from_token("hosty", "hello")
        update_and_persist_config(DEFAULT_SECTION, config)
        config = get_config()
    assert os.path.exists(cfg_file)
    assert not os.path.exists(_get_path())
    assert config.is_valid_with_token
    assert config.host == "hosty"
    assert config.token == "hello"


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


def test_mlflow_config_constructor():
    """
    Verifies the public constructor of DatabricksConfig. No variables should be added here.
    All new properties should be optional.
    """
    conf = DatabricksConfig(TEST_HOST, TEST_USER, TEST_PASSWORD, TEST_TOKEN, insecure=False)
    assert conf.host == TEST_HOST
    assert conf.username == TEST_USER
    assert conf.password == TEST_PASSWORD
    assert conf.token == TEST_TOKEN
    assert conf.insecure is False

def test_overwrite_config_creates_file_with_correct_permission():
    config_path = _get_path()

    assert not os.path.exists(config_path)
    _overwrite_config(ConfigParser())
    assert os.path.exists(config_path)

    # assert mode 600 ie owner only can read write
    assert os.stat(config_path).st_mode == 0o100600


def test_overwrite_config_overwrites_permissions_to_600():
    config_path = _get_path()
    file_descriptor = os.open(config_path, os.O_CREAT | os.O_RDWR)
    os.close(file_descriptor)

    assert not os.stat(config_path).st_mode == 0o100600

    _overwrite_config(ConfigParser())

    assert os.stat(config_path).st_mode == 0o100600
