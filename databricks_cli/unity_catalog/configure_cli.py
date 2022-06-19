# Databricks CLI
# Copyright 2022 Databricks, Inc.
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

from databricks_cli.configure.config import get_config, get_profile_from_context, profile_option
from databricks_cli.configure.provider import DatabricksConfig, update_and_persist_config, \
    ProfileConfigProvider
from databricks_cli.utils import CONTEXT_SETTINGS
from databricks_cli.sdk.version import API_VERSIONS

#################  Configure Commands  #####################


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--version', show_default=True, default=None, type=click.Choice(API_VERSIONS),
              help='API version to use for unity-catalog.')
@profile_option
def configure_cli(version):
    profile = get_profile_from_context()
    config = ProfileConfigProvider(profile).get_config() if profile else get_config()
    if config:
        new_config = config
    else:
        click.echo("Using empty configuration.")
        new_config = DatabricksConfig.empty()

    new_config.uc_api_version = version
    update_and_persist_config(profile, new_config)


def register_configure_commands(cmd_group):
    cmd_group.add_command(configure_cli, name='configure')
