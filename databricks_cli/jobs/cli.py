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
from cstriggers.core.trigger import QuartzCron
from tabulate import tabulate

from databricks_cli.click_types import OutputClickType, JsonClickType, \
    JobIdClickType, ClusterIdClickType, OptionalOneOfOption
from databricks_cli.clusters.api import ClusterApi
from databricks_cli.configure.config import provide_api_client, profile_option, debug_option
from databricks_cli.jobs.api import JobsApi
from databricks_cli.utils import eat_exceptions, CLUSTER_OPTIONS, CONTEXT_SETTINGS, pretty_format, \
    json_cli_base, truncate_string
from databricks_cli.version import print_version_callback, version


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing JSON request to POST to /api/2.0/jobs/create.')
@click.option('--json', default=None, type=JsonClickType(),
              help=JsonClickType.help('/api/2.0/jobs/create'))
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def create_cli(api_client, json_file, json):
    """
    Creates a job.

    The specification for the json option can be found
    https://docs.databricks.com/api/latest/jobs.html#create
    """
    json_cli_base(json_file, json, lambda json: JobsApi(api_client).create_job(json))


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--job-id', required=True, type=JobIdClickType(), help=JobIdClickType.help)
@click.option('--json-file', default=None, type=click.Path(),
              help='File containing partial JSON request to POST to /api/2.0/jobs/reset. '
                   'For more, read full help message.')
@click.option('--json', default=None, type=JsonClickType(),
              help='Partial JSON string to POST to /api/2.0/jobs/reset. '
                   'For more, read full help message.')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def reset_cli(api_client, json_file, json, job_id):
    """
    Resets (edits) the definition of a job.

    The specification for the json option can be found
    https://docs.databricks.com/api/latest/jobs.html#jobsjobsettings
    """
    if not bool(json_file) ^ bool(json):
        raise RuntimeError('Either --json-file or --json should be provided')
    if json_file:
        with open(json_file, 'r') as f:
            json = f.read()
    deser_json = json_loads(json)
    request_body = {
        'job_id': job_id,
        'new_settings': deser_json
    }
    JobsApi(api_client).reset_job(request_body)


def _jobs_to_table(jobs_json):
    ret = []
    for j in jobs_json['jobs']:
        ret.append((j['job_id'], truncate_string(j['settings']['name'])))
    return sorted(ret, key=lambda t: t[1].lower())


def list_all_jobs(api_client, cluster_id, cluster_name):
    jobs_api = JobsApi(api_client)
    jobs_json = jobs_api.list_jobs()

    output = jobs_json

    if cluster_id or cluster_name:
        output = {'jobs': []}
        clusters_api = ClusterApi(api_client)
        if cluster_name:
            cluster_id = clusters_api.get_cluster_id_for_name(cluster_name)

        for job in jobs_json.get('jobs'):
            settings = job.get('settings')
            if settings.get('existing_cluster_id') == cluster_id:
                output['jobs'].append(job)

    return output


def get_next_runs(jobs_data):
    start_iso = '2019-01-01T00:00:00'
    end_iso = '2025-01-01T00:00:00'
    jobs = jobs_data['jobs']
    runs = {}
    for job in jobs:
        if 'schedule' in job['settings'] and \
                'quartz_cron_expression' in job['settings']['schedule']:
            expr = job['settings']['schedule']['quartz_cron_expression'].replace("*", "0")
            expr = f'{expr} 2020-2030'
            cron = QuartzCron(schedule_string=expr, start_date=start_iso, end_date=end_iso)
            next_run = cron.next_trigger(isoformat=True)
            runs[job['settings']['name']] = next_run.replace('2020-01-01T', '')

    return runs


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Lists the jobs in the Databricks Job Service.')
@click.option('--cluster-id', cls=OptionalOneOfOption, one_of=CLUSTER_OPTIONS,
              type=ClusterIdClickType(), default=None, help=ClusterIdClickType.help,
              required=False)
@click.option('--cluster-name', cls=OptionalOneOfOption, one_of=CLUSTER_OPTIONS,
              type=ClusterIdClickType(), default=None, help=ClusterIdClickType.help,
              required=False)
@click.option('--output', '-o', 'output_type', default=None,
              help=OutputClickType.help, type=OutputClickType())
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def list_cli(api_client, cluster_id, cluster_name, output_type):
    """
    Lists the jobs in the Databricks Job Service.

    By default the output format will be a human readable table with the following fields

      - Job ID

      - Job name

    A JSON formatted output can also be requested by setting the --output parameter to "JSON"

    In table mode, the jobs are sorted by their name.
    """

    output = list_all_jobs(api_client=api_client, cluster_id=cluster_id, cluster_name=cluster_name)
    if OutputClickType.is_json(output_type):
        click.echo(pretty_format(output))
    else:
        click.echo(tabulate(_jobs_to_table(output), tablefmt='plain', disable_numparse=True))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Deletes the specified job.')
@click.option('--job-id', required=True, type=JobIdClickType(), help=JobIdClickType.help)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def delete_cli(api_client, job_id):
    """
    Deletes the specified job.
    """
    JobsApi(api_client).delete_job(job_id)


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--job-id', required=True, type=JobIdClickType(), help=JobIdClickType.help)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def get_cli(api_client, job_id):
    """
    Describes the metadata for a job.
    """
    click.echo(pretty_format(JobsApi(api_client).get_job(job_id)))


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--job-id', required=True, type=JobIdClickType(), help=JobIdClickType.help)
@click.option('--jar-params', default=None, type=JsonClickType(),
              help='JSON string specifying an array of parameters. i.e. ["param1", "param2"]')
@click.option('--notebook-params', default=None, type=JsonClickType(),
              help='JSON string specifying a map of key-value pairs. '
                   'i.e. {"name": "john doe", "age": 35}')
@click.option('--python-params', default=None, type=JsonClickType(),
              help='JSON string specifying an array of parameters. i.e. ["param1", "param2"]')
@click.option('--spark-submit-params', default=None, type=JsonClickType(),
              help='JSON string specifying an array of parameters. i.e. '
                   '["--class", "org.apache.spark.examples.SparkPi"]')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def run_now_cli(api_client, job_id, jar_params, notebook_params, python_params,
                spark_submit_params):
    """
    Runs a job with optional per-run parameters.

    Parameter options are specified in json and the format is documented in
    https://docs.databricks.com/api/latest/jobs.html#jobsrunnow.
    """
    jar_params_json = json_loads(jar_params) if jar_params else None
    notebook_params_json = json_loads(notebook_params) if notebook_params else None
    python_params = json_loads(python_params) if python_params else None
    spark_submit_params = json_loads(spark_submit_params) if spark_submit_params else None
    res = JobsApi(api_client).run_now(
        job_id, jar_params_json, notebook_params_json, python_params, spark_submit_params)
    click.echo(pretty_format(res))


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--job-id', required=True, type=JobIdClickType(), help=JobIdClickType.help)
@click.option('--job-name', required=True, help=JobIdClickType.help)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def clone_cli(api_client, job_id, job_name):
    """
    Clones an existing job
    """
    click.echo(pretty_format(JobsApi(api_client).clone_job(job_id, job_name)))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Lists the next run time for scheduled jobs.")
@click.option('--cluster-id', cls=OptionalOneOfOption, one_of=CLUSTER_OPTIONS,
              type=ClusterIdClickType(), default=None, help=ClusterIdClickType.help,
              required=False)
@click.option('--cluster-name', cls=OptionalOneOfOption, one_of=CLUSTER_OPTIONS,
              type=ClusterIdClickType(), default=None, help=ClusterIdClickType.help,
              required=False)
@click.option('--output', '-o', 'output_type', default=None,
              help=OutputClickType.help, type=OutputClickType())
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def next_runs_cli(api_client, cluster_id, cluster_name, output_type):
    """
    Lists the next run time for scheduled jobs.

    Parameter options are specified in json and the format is documented in
    https://docs.databricks.com/api/latest/jobs.html#jobsrunnow.
    """
    jobs_data = list_all_jobs(api_client=api_client,
                              cluster_id=cluster_id, cluster_name=cluster_name)

    output = get_next_runs(jobs_data=jobs_data)
    if OutputClickType.is_json(output_type):
        click.echo(pretty_format(output))
    else:
        click.echo(tabulate(_jobs_to_table(output), tablefmt='plain', disable_numparse=True))


@click.group(context_settings=CONTEXT_SETTINGS,
             short_help='Utility to interact with jobs.')
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@debug_option
@profile_option
@eat_exceptions
def jobs_group():  # pragma: no cover
    """
    Utility to interact with jobs.

    This is a wrapper around the jobs API (https://docs.databricks.com/api/latest/jobs.html).
    Job runs are handled by ``databricks runs``.
    """
    pass


jobs_group.add_command(create_cli, name='create')
jobs_group.add_command(clone_cli, name='clone')
jobs_group.add_command(delete_cli, name='delete')
jobs_group.add_command(get_cli, name='get')
jobs_group.add_command(list_cli, name='list')
jobs_group.add_command(next_runs_cli, name='next-runs')
jobs_group.add_command(reset_cli, name='reset')
jobs_group.add_command(run_now_cli, name='run-now')
