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

import ConfigParser
import sys
import os

from os.path import expanduser, join

import six

from databricks_cli.utils import error_and_quit
from databricks_cli.sdk import ApiClient, DbfsService, WorkspaceService, JobsService, \
    ClusterService, ManagedLibraryService

DEFAULT_SECTION = 'DEFAULT'
HOST = 'host'
USERNAME = 'username'
PASSWORD = 'password' #  NOQA
TOKEN = 'token'


def require_config(function):
    @six.wraps(function)
    def decorator(*args, **kwargs):
        config = DatabricksConfig.fetch_from_fs()
        if not config.is_valid:
            error_and_quit(('You haven\'t configured the CLI yet! '
                            'Please configure by entering `{} configure`').format(sys.argv[0]))
        return function(*args, **kwargs)
    decorator.__doc__ = function.__doc__
    return decorator


def _get_api_client():
    conf = DatabricksConfig.fetch_from_fs()
    if conf.is_valid_with_token:
        return ApiClient(host=conf.host, token=conf.token)
    return ApiClient(user=conf.username, password=conf.password, host=conf.host)


def get_dbfs_client():
    api_client = _get_api_client()
    return DbfsService(api_client)


def get_workspace_client():
    api_client = _get_api_client()
    return WorkspaceService(api_client)


def get_jobs_client():
    api_client = _get_api_client()
    return JobsService(api_client)


def get_clusters_client():
    api_client = _get_api_client()
    return ClusterService(api_client)


def get_libraries_client():
    api_client = _get_api_client()
    return ManagedLibraryService(api_client)


class DatabricksConfig(object):
    home = expanduser('~')

    def __init__(self):
        self._config = ConfigParser.RawConfigParser()

    def overwrite(self):
        config_path = self.get_path()
        with open(config_path, 'wb') as cfg:
            self._config.write(cfg)
        os.chmod(config_path, 0o600)

    @property
    def is_valid(self):
        return self.is_valid_with_password or self.is_valid_with_token

    @property
    def is_valid_with_password(self):
        return self._config.has_option(DEFAULT_SECTION, USERNAME) and \
            self._config.has_option(DEFAULT_SECTION, PASSWORD) and \
            self._config.has_option(DEFAULT_SECTION, HOST)

    @property
    def is_valid_with_token(self):
        return self._config.has_option(DEFAULT_SECTION, TOKEN) and \
            self._config.has_option(DEFAULT_SECTION, HOST)

    @property
    def host(self):
        return self._config.get(DEFAULT_SECTION, HOST) if self.is_valid else None

    @property
    def username(self):
        return self._config.get(DEFAULT_SECTION, USERNAME) if self.is_valid_with_password else None

    @property
    def password(self):
        return self._config.get(DEFAULT_SECTION, PASSWORD) if self.is_valid_with_password else None

    @property
    def token(self):
        return self._config.get(DEFAULT_SECTION, TOKEN) if self.is_valid_with_token else None

    @classmethod
    def fetch_from_fs(cls):
        databricks_config = cls()
        databricks_config._config.read(cls.get_path())
        return databricks_config

    @classmethod
    def construct_from_password(cls, host, username, password):
        databricks_config = cls()
        databricks_config._config.set(DEFAULT_SECTION, HOST, host)
        databricks_config._config.set(DEFAULT_SECTION, USERNAME, username)
        databricks_config._config.set(DEFAULT_SECTION, PASSWORD, password)
        return databricks_config

    @classmethod
    def construct_from_token(cls, host, token):
        databricks_config = cls()
        databricks_config._config.set(DEFAULT_SECTION, HOST, host)
        databricks_config._config.set(DEFAULT_SECTION, TOKEN, token)
        return databricks_config

    @classmethod
    def get_path(cls):
        return join(cls.home, '.databrickscfg')
