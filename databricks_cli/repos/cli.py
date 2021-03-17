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

from databricks_cli.configure.config import provide_api_client, profile_option, debug_option
from databricks_cli.repos.api import ReposApi
from databricks_cli.utils import CONTEXT_SETTINGS, eat_exceptions, pretty_format
from databricks_cli.version import print_version_callback, version


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Checkout the given branch of repository')
@click.option('--repos-id', required=True, help="Repository ID (you can find it in UI)")
@click.option('--branch', required=True, help="Branch name")
@debug_option
@profile_option
@eat_exceptions  # noqa
@provide_api_client
def update_repo_cli(api_client, repos_id, branch):
    """
    Checkout and updates given branch of the repository
    This call returns the error if branch or repository doesn't exist
    """
    content = ReposApi(api_client).update(repos_id, branch)
    click.echo(pretty_format(content))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Find Repos ID by path.')
@click.argument('path')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def get_repos_id_cli(api_client, path): # NOQA
    """
    Finds Repos ID by path, like, '/Repos/user/...'
    """
    repo_id = ReposApi(api_client).get_repos_id(path)
    click.echo(repo_id)


@click.group(context_settings=CONTEXT_SETTINGS,
             short_help="Utility to interact with Repos.")
@click.option("--version", "-v", is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@debug_option
@profile_option
@eat_exceptions
def repos_group():  # pragma: no cover
    """Utility to interact with Repos."""
    pass


repos_group.add_command(update_repo_cli, name='update')
repos_group.add_command(get_repos_id_cli, name='get-repos-id')
