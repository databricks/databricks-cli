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
from databricks_cli.configure.provider import DatabricksConfig, DEFAULT_SECTION, \
    update_and_persist_config, get_config_for_profile

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


def test_update_and_persist_config_case_insensitive():
    config = DatabricksConfig.from_token(TEST_HOST, TEST_TOKEN)
    update_and_persist_config(TEST_PROFILE, config)
    config = DatabricksConfig.from_password(TEST_HOST, TEST_USER, TEST_PASSWORD)
    update_and_persist_config(TEST_PROFILE.upper(), config)

    config = get_config_for_profile(TEST_PROFILE)
    assert config.is_valid_with_password
    assert not config.is_valid_with_token


def test_get_config_for_profile_empty():
    config = get_config_for_profile(TEST_PROFILE)
    assert not config.is_valid_with_password
    assert not config.is_valid_with_token
    assert config.host is None
    assert config.username is None
    assert config.password is None
    assert config.token is None


class TestDatabricksConfig(object):
    def test_from_token(self):
        config = DatabricksConfig.from_token(TEST_HOST, TEST_TOKEN)
        assert config.host == TEST_HOST
        assert config.token == TEST_TOKEN

    def test_from_password(self):
        config = DatabricksConfig.from_password(TEST_HOST, TEST_USER, TEST_PASSWORD)
        assert config.host == TEST_HOST
        assert config.username == TEST_USER
        assert config.password == TEST_PASSWORD

    def test_is_valid_with_token(self):
        config = DatabricksConfig.from_token(TEST_HOST, TEST_TOKEN)
        assert not config.is_valid_with_password
        assert config.is_valid_with_token

    def test_is_valid_with_password(self):
        config = DatabricksConfig.from_password(TEST_HOST, TEST_USER, TEST_PASSWORD)
        assert config.is_valid_with_password
        assert not config.is_valid_with_token

    def test_is_valid(self):
        config = DatabricksConfig.from_password(TEST_HOST, TEST_USER, TEST_PASSWORD)
        assert config.is_valid
        config = DatabricksConfig.from_token(TEST_HOST, TEST_TOKEN)
        assert config.is_valid
