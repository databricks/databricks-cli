# Databricks CLI
# Copyright 2018 Databricks, Inc.
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

from databricks_cli.utils import eat_exceptions, CONTEXT_SETTINGS
from databricks_cli.version import print_version_callback, version
from databricks_cli.configure.config import provide_api_client, profile_option, debug_option
from databricks_cli.stack.api import StackApi

DEBUG_MODE = True


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Deploy stack given a JSON configuration of the stack')
@click.argument('config_path', type=click.Path(exists=True), required=True)
@click.option('--overwrite', '-o', is_flag=True, default=False, show_default=True,
              help='Include to overwrite existing workspace notebooks and dbfs files')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def deploy(api_client, config_path, **kwargs):
    """
    Deploy a stack to the databricks workspace given a JSON stack configuration template.
    """
    click.echo('#' * 80)
    click.echo('Deploying stack at: {} with options: {}'.format(config_path, kwargs))
    StackApi(api_client).deploy(config_path, **kwargs)
    click.echo('#' * 80)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Download stack given a JSON configuration of the stack')
@click.argument('config_path', type=click.Path(exists=True), required=True)
@click.option('--overwrite', '-o', is_flag=True, help='Include to overwrite existing'
                                                      ' notebooks in the local filesystem.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def download(api_client, config_path, **kwargs):
    """
    Download a stack to the databricks workspace given a JSON stack configuration template.
    """
    click.echo('#' * 80)
    click.echo('Downloading stack at: {} with options: {}'.format(config_path, kwargs))
    StackApi(api_client).download(config_path, **kwargs)
    click.echo('#' * 80)


@click.group(context_settings=CONTEXT_SETTINGS,
             short_help='Utility to deploy and download Databricks resource stacks.')
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@debug_option
@profile_option
def stack_group():
    """
    Utility to deploy and download Databricks resource stacks.
    """
    pass


stack_group.add_command(deploy, name='deploy')
stack_group.add_command(download, name='download')
