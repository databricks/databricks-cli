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
from tabulate import tabulate

from databricks_cli.click_types import OutputClickType, JsonClickType, RunIdClickType
from databricks_cli.utils import eat_exceptions, CONTEXT_SETTINGS, pretty_format, json_cli_base, \
    truncate_string
from databricks_cli.configure.config import require_config
from databricks_cli.runs.api import submit_run, list_runs, get_run, cancel_run
from databricks_cli.version import print_version_callback, version


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to POST to /api/2.0/jobs/runs/submit.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help)
@require_config
@eat_exceptions
def submit_cli(json_file, json):
    """
    Submits a one-time run.

    The specification for the request json can be found
    https://docs.databricks.com/api/latest/jobs.html#runs-submit
    """
    json_cli_base(json_file, json, submit_run)


def _runs_to_table(runs_json):
    ret = []
    for r in runs_json.get('runs', []):
        run_id = r.get('run_id', 'no_run_id')
        run_name = r.get('run_name', 'no_run_name')
        life_cycle_state = r.get('state', {}).get('life_cycle_state', 'n/a')
        result_state = r.get('state', {}).get('result_state', 'n/a')
        run_page_url = r.get('run_page_url', 'n/a')
        row = (run_id, truncate_string(run_name), life_cycle_state, result_state, run_page_url)
        ret.append(row)
    return ret


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--job-id', default=None, type=int,
              help='If specified, runs from only the specified job_id will be listed.')
@click.option('--active-only', is_flag=True, default=None,
              help='If specified, only active runs will be listed')
@click.option('--completed-only', is_flag=True, default=None,
              help='If specifed, only completed runs will be listed')
@click.option('--offset', default=None, type=int,
              help='Set to 0 by default. The offset is relative to the most recent run ID.')
@click.option('--limit', default=None, type=int,
              help='Set to 20 runs by default. The limit determines the number of runs listed. '
                   'Limit must be between 0 and 1000.')
@click.option('--output', help=OutputClickType.help, type=OutputClickType())
@require_config
@eat_exceptions # noqa
def list_cli(job_id, active_only, completed_only, offset, limit, output): # noqa
    """
    Lists job runs.

    The limit and offset determine which runs will be listed. Runs are always listed
    by descending order of run start time or run_id.

    In the TABLE output mode, the columns are as follows.

      - Run ID

      - Run name

      - Life cycle state

      - Result state
    """
    runs_json = list_runs(job_id, active_only, completed_only, offset, limit)
    if OutputClickType.is_json(output):
        click.echo(pretty_format(runs_json))
    else:
        click.echo(tabulate(_runs_to_table(runs_json), tablefmt='plain'))


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--run-id', required=True, type=RunIdClickType())
@require_config
@eat_exceptions
def get_cli(run_id):
    """
    Gets the metadata about a run in json form.

    The output schema is documented https://docs.databricks.com/api/latest/jobs.html#runs-get.
    """
    click.echo(pretty_format(get_run(run_id)))


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--run-id', required=True, type=RunIdClickType())
@require_config
@eat_exceptions
def cancel_cli(run_id):
    """
    Cancels the run specified.
    """
    click.echo(pretty_format(cancel_run(run_id)))


@click.group(context_settings=CONTEXT_SETTINGS,
             short_help='Utility to interact with the jobs/runs API endpoints.')
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@require_config
@eat_exceptions
def runs_group():
    """
    Utility to interact with jobs/runs API endpoints.
    """
    pass


runs_group.add_command(submit_cli, name='submit')
runs_group.add_command(list_cli, name='list')
runs_group.add_command(get_cli, name='get')
runs_group.add_command(cancel_cli, name='cancel')
