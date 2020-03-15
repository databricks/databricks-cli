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

from __future__ import print_function

import click
from click import UsageError
import json
import time
from datetime import datetime, timedelta
from tabulate import tabulate

from databricks_cli.utils import eat_exceptions, error_and_quit, CONTEXT_SETTINGS
from databricks_cli.version import print_version_callback, version
from databricks_cli.configure.cli import configure_cli
from databricks_cli.configure.config import provide_api_client, profile_option, debug_option
from databricks_cli.dbfs.api import DbfsApi
from databricks_cli.dbfs.dbfs_path import DbfsPath, DbfsPathClickType


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--absolute', is_flag=True, default=False,
              help='Displays absolute paths.')
@click.option('-l', is_flag=True, default=False,
              help='Displays full information including size and file type.')
@click.argument('dbfs_path', nargs=-1, type=DbfsPathClickType())
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def ls_cli(api_client, l, absolute, dbfs_path): #  NOQA
    """
    List files in DBFS.
    """
    if len(dbfs_path) == 0:
        dbfs_path = DbfsPath('dbfs:/')
    elif len(dbfs_path) == 1:
        dbfs_path = dbfs_path[0]
    else:
        error_and_quit('ls can take a maximum of one path.')
    files = DbfsApi(api_client).list_files(dbfs_path)
    table = tabulate([f.to_row(is_long_form=l, is_absolute=absolute) for f in files],
                     tablefmt='plain')
    click.echo(table)


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('dbfs_path', type=DbfsPathClickType())
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def mkdirs_cli(api_client, dbfs_path):
    """
    Make directories in DBFS.

    Mkdirs will create directories along the path to the argument directory.
    """
    DbfsApi(api_client).mkdirs(dbfs_path)


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('dbfs_path', type=DbfsPathClickType())
@click.option('--recursive', '-r', is_flag=True, default=False)
@click.option('--as-job', is_flag=True, default=False)
@click.option('--async', is_flag=True, default=False)
@click.option('--cluster-id', required=False)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def rm_cli(api_client, dbfs_path, recursive, as_job, async, cluster_id):
    """
    Remove files from dbfs.

    To remove a directory you must provide the --recursive flag.
    """
    if async or (cluster_id is not None):
        as_job = True

    if as_job:
        id = async_rm_start_impl(api_client, dbfs_path, recursive, cluster_id)
        if async:
            click.echo(async_rm_status_impl(api_client, id))
        else:
            async_rm_wait_impl(api_client, id)
    else:
        DbfsApi(api_client).delete(dbfs_path, recursive)







def async_rm_start_impl(api_client, dbfs_path, recursive, cluster_id):
    delete_job_id = DbfsApi(api_client).async_delete_start(dbfs_path, recursive, cluster_id)
    return delete_job_id['delete_job_id']


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('dbfs_path', type=DbfsPathClickType())
@click.option('--recursive', '-r', is_flag=True, default=False)
@click.option('--cluster-id', '-c', required=False)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def async_rm_start_cli(api_client, dbfs_path, recursive, cluster_id):
    """
    Start a rm-async request.

    To remove a directory you must provide the --recursive flag.
    """
    id = async_rm_start_impl(api_client, dbfs_path, recursive, cluster_id)
    click.echo(async_rm_status_impl(api_client, id))


def truncate_string(s, length=100):
    if len(s) <= length:
        return s
    return s[:length] + '...'

def parse_timestamp(ts):
    t = int(ts) / 1000
    return datetime.utcfromtimestamp(t).strftime('%Y-%m-%d %H:%M:%S')


def async_rm_status_to_row(run_json):
    r = json.loads(run_json)
    params = r['task']['notebook_task']['base_parameters']
    state = r['state']
    if 'result_state' in state:
        result = state['result_state']
        duration_ms = r['setup_duration'] + r['execution_duration'] + r['cleanup_duration']
        duration = timedelta(milliseconds=duration_ms)
    else:
        result = ""
        duration = ""

    cluster_type = 'new' if 'new_cluster' in r['cluster_spec'] else "existing"
    cluster_id = r['cluster_instance']['cluster_id'] if 'cluster_instance' in r else ""
    return (
        r['run_id'], params['path'], params['recursive'],
        r['run_page_url'], cluster_type, cluster_id,
        parse_timestamp(r['start_time']), state['life_cycle_state'], result, duration
    )


def async_rm_status_to_table(id, runs_json):
    ret = []
    if id is not None:
        r = runs_json['delete_job_run']
        ret.append(async_rm_status_to_row(r))
    else:
        for r in runs_json['delete_job_runs']:
            ret.append(async_rm_status_to_row(r))
    return ret


def async_rm_status_impl(api_client, id, limit=None):
    status = DbfsApi(api_client).async_delete_status(id, limit)
    if id is not None:
        return tabulate(async_rm_status_to_table(id, status), tablefmt="plain")
    else:
        headers = (
            "id", "path", "recursive",
            "run_page_url", "cluster_type", "cluster_id",
            "start_time", "state", "result", "duration")
        return tabulate(async_rm_status_to_table(id, status), headers, tablefmt='simple')


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--id', required=False)
@click.option('--limit', required=False)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def async_rm_status_cli(api_client, id, limit):
    """
    Check the status of your rm-async request(s).
    """
    if id is not None and limit is not None:
        raise UsageError("You cannot specify both --id and --limit.")

    click.echo(async_rm_status_impl(api_client, id, limit))


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--id', required=True)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def async_rm_cancel_cli(api_client, id):
    """
    Cancel your rm-async request.
    """
    DbfsApi(api_client).async_delete_cancel(id)


def async_rm_wait_impl(api_client, id):
    i = 0
    progress_chars = ['/', '-', '\\', '|']
    while True:
        status = DbfsApi(api_client).async_delete_status(id)
        click.echo("\r" + async_rm_status_impl(api_client, id), nl=False)
        r = json.loads(status['delete_job_run'])
        if r['state'].get('result_state') is not None:
            click.echo("        ")
            break
        i = (i + 1) % len(progress_chars)
        click.echo(" " + progress_chars[i], nl=False)
        time.sleep(0.5)

@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--id', required=True)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def async_rm_wait_cli(api_client, id):
    """
    Wait until your rm-async request is complete.
    """
    async_rm_wait_impl(api_client, id)


@click.group(context_settings=CONTEXT_SETTINGS, short_help='Remove files from DBFS asynchronously.')
@debug_option
@profile_option
@eat_exceptions
def async_rm_group():
    """
    Remove files from dbfs asynchronously.
    """
    pass


async_rm_group.add_command(async_rm_start_cli, name="start")
async_rm_group.add_command(async_rm_status_cli, name="status")
async_rm_group.add_command(async_rm_cancel_cli, name="cancel")
async_rm_group.add_command(async_rm_wait_cli, name="wait")









@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--recursive', '-r', is_flag=True, default=False)
@click.option('--overwrite', is_flag=True, default=False)
@click.argument('src')
@click.argument('dst')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def cp_cli(api_client, recursive, overwrite, src, dst):
    """
    Copy files to and from DBFS.

    Note that this function will fail if the src and dst are both on the local filesystem.

    For non-recursive copies, if the dst is a directory, the file will be placed inside the
    directory. For example ``dbfs cp dbfs:/apple.txt .`` will create a file at `./apple.txt`.

    For recursive copies, files inside of the src directory will be copied inside the dst directory
    with the same name. If the dst path does not exist, a directory will be created. For example
    ``dbfs cp -r dbfs:/foo foo`` will create a directory foo and place the files ``dbfs:/foo/a`` at
    ``foo/a``. If ``foo/a`` already exists, the file will not be overriden unless the --overwrite
    flag is provided -- however, dbfs cp --recursive will continue to try and copy other files.
    """
    # Copy to DBFS in this case
    DbfsApi(api_client).cp(recursive, overwrite, src, dst)


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('src', type=DbfsPathClickType())
@click.argument('dst', type=DbfsPathClickType())
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def mv_cli(api_client, src, dst):
    """
    Moves a file between two DBFS paths.
    """
    DbfsApi(api_client).move(src, dst)


@click.group(context_settings=CONTEXT_SETTINGS, short_help='Utility to interact with DBFS.')
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@debug_option
@profile_option
def dbfs_group():
    """
    Utility to interact with DBFS.

    DBFS paths are all prefixed with dbfs:/. Local paths can be absolute or local.
    """
    pass


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('src')
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def cat_cli(api_client, src):
    """
    Show the contents of a file. Does not work for directories.
    """
    DbfsApi(api_client).cat(src)



@click.group(context_settings=CONTEXT_SETTINGS, short_help='Perform asynchronous DBFS operations.')
@debug_option
@profile_option
@eat_exceptions
def async_group():
    """
    Remove files from dbfs asynchronously.
    """
    pass


async_group.add_command(async_rm_group, name='rm')


dbfs_group.add_command(configure_cli, name='configure')
dbfs_group.add_command(ls_cli, name='ls')
dbfs_group.add_command(mkdirs_cli, name='mkdirs')
dbfs_group.add_command(rm_cli, name='rm')
dbfs_group.add_command(async_group, name='async')
dbfs_group.add_command(cp_cli, name='cp')
dbfs_group.add_command(mv_cli, name='mv')
dbfs_group.add_command(cat_cli, name='cat')
