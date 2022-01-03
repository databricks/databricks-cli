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

from abc import abstractmethod, ABCMeta
from configparser import ConfigParser
import os
from os.path import expanduser, join

from databricks_cli.utils import InvalidConfigurationError


_home = expanduser('~')
CONFIG_FILE_ENV_VAR = "DATABRICKS_CONFIG_FILE"
HOST = 'host'
USERNAME = 'username'
PASSWORD = 'password' # NOQA
TOKEN = 'token'
INSECURE = 'insecure'
JOBS_API_VERSION = 'jobs-api-version'
USE_AZURE_CLI_AUTH = 'azure-cli-auth'
USE_AZURE_MSI_AUTH = 'azure-msi-auth'
AZURE_ENVIRONMENT = 'azure-environment'
AZURE_TENANT_ID = 'azure-tenant-id'
AZURE_CLIENT_ID = 'azure-client-id'
AZURE_CLIENT_SECRET = 'azure-client-secret'
AZURE_DATABRICKS_RESOURCE_ID = 'azure-resource-id'
AZURE_DEFAULT_AD_ENDPOINT = "https://login.microsoftonline.com"
DEFAULT_SECTION = 'DEFAULT'
AZURE_ENVIRONMENTS = {
    'public': "https://login.microsoftonline.com",
    'china': 'https://login.chinacloudapi.cn',
    'usgovernment': 'https://login.microsoftonline.us',
    'german': 'https://login.microsoftonline.de',
}

# User-provided override for the DatabricksConfigProvider
_config_provider = None


def _get_path():
    return os.environ.get(CONFIG_FILE_ENV_VAR, join(_home, '.databrickscfg'))


def _fetch_from_fs():
    raw_config = ConfigParser()
    raw_config.read(_get_path())
    return raw_config


def _create_section_if_absent(raw_config, profile):
    if not raw_config.has_section(profile) and profile != DEFAULT_SECTION:
        raw_config.add_section(profile)


def _get_option_if_exists(raw_config, profile, option):
    if profile == DEFAULT_SECTION:
        # We must handle the DEFAULT_SECTION differently since it is not in the _sections property
        # of raw config.
        return raw_config.get(profile, option) if raw_config.has_option(profile, option) else None
    # Check if option is defined in the profile.
    elif option not in raw_config._sections.get(profile, {}).keys():
        return None
    return raw_config.get(profile, option)


def _set_option(raw_config, profile, option, value):
    if value:
        raw_config.set(profile, option, value)
    else:
        raw_config.remove_option(profile, option)


def _overwrite_config(raw_config):
    config_path = _get_path()
    with open(config_path, 'w') as cfg:
        raw_config.write(cfg)
    os.chmod(config_path, 0o600)


def update_and_persist_config(profile, databricks_config):
    """
    Takes a DatabricksConfig and adds the in memory contents to the persisted version of the
    config. This will overwrite any other config that was persisted to the file system under the
    same profile.
    :param databricks_config: DatabricksConfig
    """
    profile = profile if profile else DEFAULT_SECTION
    raw_config = _fetch_from_fs()
    _create_section_if_absent(raw_config, profile)
    _set_option(raw_config, profile, HOST, databricks_config.host)
    if databricks_config.username:
        _set_option(raw_config, profile, USERNAME, databricks_config.username)
    if databricks_config.password:
        _set_option(raw_config, profile, PASSWORD, databricks_config.password)
    if databricks_config.token:
        _set_option(raw_config, profile, TOKEN, databricks_config.token)
    _set_option(raw_config, profile, INSECURE, databricks_config.insecure)
    _set_option(raw_config, profile, JOBS_API_VERSION, databricks_config.jobs_api_version)
    if databricks_config.use_azure_cli_auth:
        _set_option(raw_config, profile, USE_AZURE_CLI_AUTH, "true")
    if databricks_config.use_azure_msi_auth:
        _set_option(raw_config, profile, USE_AZURE_MSI_AUTH, "true")
        if databricks_config.azure_resource_id:
            _set_option(raw_config, profile, AZURE_DATABRICKS_RESOURCE_ID, databricks_config.azure_resource_id)
    if databricks_config.azure_client_id and databricks_config.azure_client_secret \
            and databricks_config.azure_tenant_id:
        _set_option(raw_config, profile, AZURE_CLIENT_ID, databricks_config.azure_client_id)
        _set_option(raw_config, profile, AZURE_CLIENT_SECRET, databricks_config.azure_client_secret)
        _set_option(raw_config, profile, AZURE_TENANT_ID, databricks_config.azure_tenant_id)
        if databricks_config.azure_resource_id:
            _set_option(raw_config, profile, AZURE_DATABRICKS_RESOURCE_ID, databricks_config.azure_resource_id)
        if databricks_config.azure_environment:
            _set_option(raw_config, profile, AZURE_ENVIRONMENT, databricks_config.azure_environment)

    _overwrite_config(raw_config)


def get_config():
    """
    Returns a DatabricksConfig containing the hostname and authentication used to talk to
    the Databricks API. By default, we leverage the DefaultConfigProvider to get
    this config, but this behavior may be overridden by calling 'set_config_provider'

    If no DatabricksConfig can be found, an InvalidConfigurationError will be raised.
    """
    global _config_provider
    if _config_provider:
        config = _config_provider.get_config()
        if config:
            return config
        raise InvalidConfigurationError(
            'Custom provider returned no DatabricksConfig: %s' % _config_provider)

    config = DefaultConfigProvider().get_config()
    if config:
        return config
    raise InvalidConfigurationError.for_profile(None)


def get_config_for_profile(profile):
    """
    [Deprecated] Reads from the filesystem and gets a DatabricksConfig for the
    specified profile. If it does not exist, then return a DatabricksConfig with fields set
    to None.

    Internal callers should prefer get_config() to use user-specified overrides, and
    to return appropriate error messages as opposited to invalid configurations.

    If you want to read from a specific profile, please instead use
    'ProfileConfigProvider(profile).get_config()'.

    This method is maintained for backwards-compatibility. It may be removed in future versions.

    :return: DatabricksConfig
    """
    profile = profile if profile else DEFAULT_SECTION
    config = EnvironmentVariableConfigProvider().get_config()
    if config and config.is_valid:
        return config

    config = ProfileConfigProvider(profile).get_config()
    if config:
        return config
    return DatabricksConfig.empty()


def set_config_provider(provider):
    """
    Sets a DatabricksConfigProvider that will be used for all future calls to get_config(),
    used by the Databricks CLI code to discover the user's credentials.
    """
    global _config_provider
    if provider and not isinstance(provider, DatabricksConfigProvider):
        raise Exception('Must be instance of DatabricksConfigProvider: %s' % _config_provider)
    _config_provider = provider


def get_config_provider():
    """
    Returns the current DatabricksConfigProvider.
    If None, the DefaultConfigProvider will be used.
    """
    global _config_provider
    return _config_provider


class DatabricksConfigProvider(object):
    """
    Responsible for providing hostname and authentication information to make
    API requests against the Databricks REST API.
    This method should generally return None if it cannot provide credentials, in order
    to facilitate chanining of providers.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_config(self):
        pass


class DefaultConfigProvider(DatabricksConfigProvider):
    """Look for credentials in a chain of default locations."""
    def __init__(self):
        self._providers = (
            SparkTaskContextConfigProvider(),
            EnvironmentVariableConfigProvider(),
            ProfileConfigProvider()
        )

    def get_config(self):
        for provider in self._providers:
            config = provider.get_config()
            if config is not None and config.is_valid:
                return config
        return None


class SparkTaskContextConfigProvider(DatabricksConfigProvider):
    """Loads credentials from Spark TaskContext if running in a Spark Executor."""

    @staticmethod
    def _get_spark_task_context_or_none():
        try:
            from pyspark import TaskContext  # pylint: disable=import-error
            return TaskContext.get()
        except ImportError:
            return None

    @staticmethod
    def set_insecure(x):
        from pyspark import SparkContext  # pylint: disable=import-error      
        new_val = "True" if x else None
        SparkContext._active_spark_context.setLocalProperty("spark.databricks.ignoreTls", new_val)

    def get_config(self):
        context = self._get_spark_task_context_or_none()
        if context is not None:
            host = context.getLocalProperty("spark.databricks.api.url")
            token = context.getLocalProperty("spark.databricks.token")
            insecure = context.getLocalProperty("spark.databricks.ignoreTls")
            config = DatabricksConfig.from_token(host=host, token=token, insecure=insecure)
            if config.is_valid:
                return config
        return None


class EnvironmentVariableConfigProvider(DatabricksConfigProvider):
    """Loads from system environment variables."""
    def get_config(self):
        host = os.environ.get('DATABRICKS_HOST')
        username = os.environ.get('DATABRICKS_USERNAME')
        password = os.environ.get('DATABRICKS_PASSWORD')
        token = os.environ.get('DATABRICKS_TOKEN')
        insecure = os.environ.get('DATABRICKS_INSECURE')
        jobs_api_version = os.environ.get('DATABRICKS_JOBS_API_VERSION')
        use_azure_cli_auth = os.environ.get('DATABRICKS_USE_AZURE_CLI_AUTH') is not None
        use_azure_msi_auth = os.environ.get('ARM_USE_MSI') is not None
        azure_client_id = os.environ.get('ARM_CLIENT_ID')
        azure_client_secret = os.environ.get('ARM_CLIENT_SECRET')
        azure_tenant_id = os.environ.get('ARM_TENANT_ID')
        azure_environment = os.environ.get('ARM_ENVIRONMENT')
        azure_resource_id = os.environ.get('DATABRICKS_AZURE_RESOURCE_ID')
        config = DatabricksConfig(host, username, password, token, insecure, jobs_api_version, use_azure_cli_auth,
                                  use_azure_msi_auth, azure_environment, azure_tenant_id, azure_client_id,
                                  azure_client_secret, azure_resource_id)
        if config.is_valid:
            return config
        return None


class ProfileConfigProvider(DatabricksConfigProvider):
    """Loads from the databrickscfg file."""
    def __init__(self, profile=DEFAULT_SECTION):
        self.profile = profile

    def get_config(self):
        raw_config = _fetch_from_fs()
        host = _get_option_if_exists(raw_config, self.profile, HOST)
        username = _get_option_if_exists(raw_config, self.profile, USERNAME)
        password = _get_option_if_exists(raw_config, self.profile, PASSWORD)
        token = _get_option_if_exists(raw_config, self.profile, TOKEN)
        insecure = _get_option_if_exists(raw_config, self.profile, INSECURE)
        jobs_api_version = _get_option_if_exists(raw_config, self.profile, JOBS_API_VERSION)
        use_azure_cli_auth = _get_option_if_exists(raw_config, self.profile, USE_AZURE_CLI_AUTH)
        use_azure_msi_auth = _get_option_if_exists(raw_config, self.profile, USE_AZURE_MSI_AUTH)
        azure_environment = _get_option_if_exists(raw_config, self.profile, AZURE_ENVIRONMENT)
        azure_tenant_id = _get_option_if_exists(raw_config, self.profile, AZURE_TENANT_ID)
        azure_client_id = _get_option_if_exists(raw_config, self.profile, AZURE_CLIENT_ID)
        azure_client_secret = _get_option_if_exists(raw_config, self.profile, AZURE_CLIENT_SECRET)
        azure_resource_id = _get_option_if_exists(raw_config, self.profile, AZURE_DATABRICKS_RESOURCE_ID)
        config = DatabricksConfig(host, username, password, token, insecure, jobs_api_version, use_azure_cli_auth,
                                  use_azure_msi_auth, azure_environment, azure_tenant_id, azure_client_id,
                                  azure_client_secret, azure_resource_id)
        if config.is_valid:
            return config
        return None


class DatabricksConfig(object):
    def __init__(self, host, username, password, token, insecure,
                 jobs_api_version=None, use_azure_cli_auth=False,
                 use_azure_msi_auth=None, azure_environment=None, azure_tenant_id=None,
                 azure_client_id=None, azure_client_secret=None, azure_resource_id=None):  # noqa
        self.host = host
        self.username = username
        self.password = password
        self.token = token
        self.insecure = insecure
        self.jobs_api_version = jobs_api_version
        self.use_azure_cli_auth = use_azure_cli_auth
        self.use_azure_msi_auth = use_azure_msi_auth
        self.azure_environment = (azure_environment or "public").lower()
        self.azure_tenant_id = azure_tenant_id
        self.azure_client_id = azure_client_id
        self.azure_client_secret = azure_client_secret
        self.azure_resource_id = azure_resource_id

    @classmethod
    def from_token(cls, host, token, insecure=None, jobs_api_version=None):
        return DatabricksConfig(host, None, None, token, insecure, jobs_api_version)

    @classmethod
    def using_azure_cli_auth(cls, host, insecure=None, jobs_api_version=None):
        return DatabricksConfig(host, None, None, None, insecure, jobs_api_version, use_azure_cli_auth=True)

    @classmethod
    def using_azure_msi_auth(cls, host, resource_id=None, insecure=None, jobs_api_version=None):
        return DatabricksConfig(host, None, None, None, insecure, jobs_api_version, use_azure_msi_auth=True,
                                azure_resource_id=resource_id)

    @classmethod
    def from_password(cls, host, username, password, insecure=None, jobs_api_version=None):
        return DatabricksConfig(host, username, password, None, insecure, jobs_api_version)

    @classmethod
    def for_azure_spn(cls, host, client_id, client_secret, tenant_id, resource_id=None,
                      azure_env=None, insecure=None, jobs_api_version=None):
        return DatabricksConfig(host, None, None, None, insecure, jobs_api_version,
                                azure_client_id=client_id, azure_client_secret=client_secret,
                                azure_tenant_id=tenant_id, azure_resource_id=resource_id,
                                azure_environment=azure_env)

    @classmethod
    def empty(cls):
        return DatabricksConfig(None, None, None, None, None, None)

    @property
    def is_valid_with_token(self):
        return self.host is not None and self.token is not None

    @property
    def is_valid_with_password(self):
        return self.host is not None and self.username is not None and self.password is not None

    @property
    def is_valid_with_azure_cli_auth(self):
        return self.host is not None and self.use_azure_cli_auth is not None and self.use_azure_cli_auth

    @property
    def is_valid_with_azure_msi_auth(self):
        return self.host is not None and self.use_azure_msi_auth is not None and self.use_azure_msi_auth

    @property
    def is_valid_with_azure_client_auth(self):
        return self.host is not None and self.azure_tenant_id is not None \
               and self.azure_client_id is not None and self.azure_client_secret is not None

    @property
    def is_valid(self):
        return self.is_valid_with_azure_cli_auth or self.is_valid_with_azure_msi_auth \
               or self.is_valid_with_azure_client_auth \
               or self.is_valid_with_token or self.is_valid_with_password

