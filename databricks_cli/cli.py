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

from databricks_cli.configure.config import profile_option, debug_option
from databricks_cli.libraries.cli import libraries_group
from databricks_cli.version import print_version_callback, version
from databricks_cli.utils import CONTEXT_SETTINGS
from databricks_cli.configure.cli import configure_cli
from databricks_cli.dbfs.cli import dbfs_group
from databricks_cli.workspace.cli import workspace_group
from databricks_cli.jobs.cli import jobs_group
from databricks_cli.clusters.cli import clusters_group
from databricks_cli.runs.cli import runs_group
from databricks_cli.secrets.cli import secrets_group
from databricks_cli.stack.cli import stack_group
from databricks_cli.groups.cli import groups_group
from databricks_cli.instance_pools.cli import instance_pools_group
from databricks_cli.pipelines.cli import pipelines_group


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@debug_option
@profile_option
def cli():
    pass


cli.add_command(configure_cli, name='configure')
cli.add_command(dbfs_group, name='fs')
cli.add_command(workspace_group, name='workspace')
cli.add_command(jobs_group, name='jobs')
cli.add_command(clusters_group, name='clusters')
cli.add_command(runs_group, name='runs')
cli.add_command(libraries_group, name='libraries')
cli.add_command(secrets_group, name='secrets')
cli.add_command(stack_group, name='stack')
cli.add_command(groups_group, name='groups')
cli.add_command(instance_pools_group, name="instance-pools")
cli.add_command(pipelines_group, name='pipelines')

if __name__ == "__main__":
    cli()
