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

import click
import six

from databricks_cli.utils import error_and_quit
from databricks_cli.sdk import ApiClient, DbfsService, WorkspaceService, JobsService, ClusterService

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


def profile_option(f):
    return click.option('--profile', required=False, default=DEFAULT_SECTION)(f)


def _get_api_client(profile):
    conf = DatabricksConfig.fetch_from_fs()
    if conf.is_valid_with_token:
        return ApiClient(host=conf.host(profile), token=conf.token(profile))
    return ApiClient(user=conf.username(profile), password=conf.password(profile),
                     host=conf.host(profile))


def get_dbfs_client(profile=DEFAULT_SECTION):
    api_client = _get_api_client(profile)
    return DbfsService(api_client)


def get_workspace_client(profile=DEFAULT_SECTION):
    api_client = _get_api_client(profile)
    return WorkspaceService(api_client)


def get_jobs_client(profile=DEFAULT_SECTION):
    api_client = _get_api_client(profile)
    return JobsService(api_client)


def get_clusters_client(profile=DEFAULT_SECTION):
    api_client = _get_api_client(profile)
    return ClusterService(api_client)


class DatabricksConfig(object):
    home = expanduser('~')

    def __init__(self):
        self._config = ConfigParser.RawConfigParser()

    def overwrite(self):
        config_path = self.get_path()
        with open(config_path, 'wb') as cfg:
            self._config.write(cfg)
        os.chmod(config_path, 0o600)

    def is_valid(self, profile):
        return self.is_valid_with_password(profile) or self.is_valid_with_token(profile)

    def is_valid_with_password(self, profile):
        return self._config.has_option(profile, USERNAME) and \
            self._config.has_option(profile, PASSWORD) and \
            self._config.has_option(profile, HOST)

    def is_valid_with_token(self, profile):
        return self._config.has_option(profile, TOKEN) and \
            self._config.has_option(profile, HOST)

    def host(self, profile):
        return self._config.get(profile, HOST) if self.is_valid(profile) else None

    def username(self, profile):
        return self._config.get(profile, USERNAME) if self.is_valid_with_password(profile) else None

    def password(self, profile):
        return self._config.get(profile, PASSWORD) if self.is_valid_with_password(profile) else None

    def token(self, profile):
        return self._config.get(profile, TOKEN) if self.is_valid_with_token(profile) else None

    @classmethod
    def fetch_from_fs(cls):
        databricks_config = cls()
        databricks_config._config.read(cls.get_path())
        return databricks_config

    def _create_section_if_absent(self, profile):
        if not self._config.has_section(profile) and profile != DEFAULT_SECTION:
            self._config.add_section(profile)

    def update_with_password(self, profile, host, username, password):
        self._create_section_if_absent(profile)
        self._config.set(profile, HOST, host)
        self._config.set(profile, USERNAME, username)
        self._config.set(profile, PASSWORD, password)
        return self

    def update_with_token(self, profile, host, token):
        self._create_section_if_absent(profile)
        self._config.set(profile, HOST, host)
        self._config.set(profile, TOKEN, token)
        return self

    @classmethod
    def get_path(cls):
        return join(cls.home, '.databrickscfg')
