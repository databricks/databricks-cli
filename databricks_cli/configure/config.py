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

from databricks_cli.utils import error_and_quit
from databricks_cli.sdk import ApiClient, DbfsService

DEFAULT_SECTION = 'DEFAULT'
HOST = 'host'
USERNAME = 'username'
PASSWORD = 'password' #  NOQA


def require_config(function):
    def decorator(*args, **kwargs):
        config = DatabricksConfig.fetch_from_fs()
        if not config.is_valid:
            error_and_quit(('You haven\'t configured the CLI yet! '
                            'Please configure by entering `{} configure`').format(sys.argv[0]))
        return function(*args, **kwargs)
    decorator.__doc__ = function.__doc__
    return decorator


def get_dbfs_client():
    conf = DatabricksConfig.fetch_from_fs()
    return DbfsService(ApiClient(conf.username, conf.password, host=conf.host))


class DatabricksConfig(object):
    home = expanduser('~')

    def __init__(self, host=None, username=None, password=None):
        self.config = ConfigParser.RawConfigParser()
        if host:
            self.config.set(DEFAULT_SECTION, HOST, host)
        if username:
            self.config.set(DEFAULT_SECTION, USERNAME, username)
        if password:
            self.config.set(DEFAULT_SECTION, PASSWORD, password)

    def overwrite(self):
        config_path = join(self.home, '.databrickscfg')
        with open(config_path, 'wb') as cfg:
            self.config.write(cfg)
        os.chmod(config_path, 0o600)

    @property
    def is_valid(self):
        return self.config.has_option(DEFAULT_SECTION, USERNAME) and \
            self.config.has_option(DEFAULT_SECTION, PASSWORD) and \
            self.config.has_option(DEFAULT_SECTION, HOST)

    @property
    def host(self):
        return self.config.get(DEFAULT_SECTION, HOST) if self.is_valid else None

    @property
    def username(self):
        return self.config.get(DEFAULT_SECTION, USERNAME) if self.is_valid else None

    @property
    def password(self):
        return self.config.get(DEFAULT_SECTION, PASSWORD) if self.is_valid else None

    @classmethod
    def fetch_from_fs(cls):
        databricks_config = cls()
        databricks_config.config.read(cls.get_path())
        return databricks_config

    @classmethod
    def get_path(cls):
        return join(cls.home, '.databrickscfg')
