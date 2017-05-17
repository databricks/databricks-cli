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

import click

from click import ParamType

from databricks_cli.utils import CONTEXT_SETTINGS
from databricks_cli.configure.config import DatabricksConfig


PROMPT_HOST = 'Databricks Host (should begin with https://)'
PROMPT_USERNAME = 'Username'
PROMPT_PASSWORD = 'Password' #  NOQA


@click.command(context_settings=CONTEXT_SETTINGS)
def configure_cli():
    conf = DatabricksConfig.fetch_from_fs()
    if conf.password:
        default_password = '*' * len(conf.password)
    else:
        default_password = None
    host = click.prompt(PROMPT_HOST, default=conf.host, type=_DbfsHost())
    username = click.prompt(PROMPT_USERNAME, default=conf.username)
    password = click.prompt(PROMPT_PASSWORD, default=default_password, hide_input=True,
                            confirmation_prompt=True)
    if password == default_password:
        password = conf.password
    config = DatabricksConfig(host, username, password)
    config.overwrite()


class _DbfsHost(ParamType):
    """
    Used to validate the configured host
    """
    def convert(self, value, param, ctx):
        if value.startswith('https://'):
            return value
        else:
            self.fail('The host does not start with https://')
