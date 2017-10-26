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

from json import loads as json_loads

import click
from tabulate import tabulate

from databricks_cli.click_types import OutputClickType
from databricks_cli.jobs.api import create_job, list_jobs, delete_job, get_job, reset_job, run_now
from databricks_cli.utils import eat_exceptions, CONTEXT_SETTINGS, pretty_format, json_cli_base
from databricks_cli.configure.config import require_config
from databricks_cli.version import print_version_callback


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--json-file', default=None,
              help='File containing json to POST to /jobs/create.')
@click.option('--json', default=None, help='Displays absolute paths.')
@require_config
@eat_exceptions
def create_cli(json_file, json):
    json_cli_base(json_file, json, create_job)


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--json-file', default=None,
              help='File containing json to POST to /jobs/create.')
@click.option('--json', default=None, help='Displays absolute paths.')
@require_config
@eat_exceptions
def reset_cli(json_file, json):
    json_cli_base(json_file, json, reset_job)


def _jobs_to_table(jobs_json):
    ret = []
    for j in jobs_json['jobs']:
        ret.append((j['job_id'], j['settings']['name']))
    return ret


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--output', default=None, help=OutputClickType.help, type=OutputClickType())
@require_config
@eat_exceptions
def list_cli(output):
    jobs_json = list_jobs()
    if OutputClickType.is_json(output):
        click.echo(pretty_format(jobs_json))
    else:
        click.echo(tabulate(_jobs_to_table(jobs_json), tablefmt='plain'))


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--job-id', required=True, type=int)
@require_config
@eat_exceptions
def delete_cli(job_id):
    delete_job(job_id)


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--job-id', required=True, type=int)
@require_config
@eat_exceptions
def get_cli(job_id):
    click.echo(pretty_format(get_job(job_id)))


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--job-id', required=True, type=int)
@click.option('--jar-params', default=None)
@click.option('--notebook-params', default=None)
@require_config
@eat_exceptions
def run_now_cli(job_id, jar_params, notebook_params):
    jar_params_json = json_loads(jar_params) if jar_params else None
    notebook_params_json = json_loads(notebook_params) if notebook_params else None
    res = run_now(job_id, jar_params_json, notebook_params_json)
    click.echo(pretty_format(res))


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True)
@require_config
@eat_exceptions
def jobs_group():
    """
    Utility to interact with the Jobs service.
    """
    pass


jobs_group.add_command(create_cli, name='create')
jobs_group.add_command(list_cli, name='list')
jobs_group.add_command(delete_cli, name='delete')
jobs_group.add_command(get_cli, name='get')
jobs_group.add_command(reset_cli, name='reset')
jobs_group.add_command(run_now_cli, name='run-now')
