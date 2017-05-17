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

import os
import click
from tabulate import tabulate
from requests.exceptions import HTTPError

from databricks_cli.utils import eat_exceptions, error_and_quit, CONTEXT_SETTINGS
from databricks_cli.version import print_version_callback
from databricks_cli.configure.cli import configure_cli
from databricks_cli.configure.config import require_config
from databricks_cli.dbfs.api import put_file, get_file, list_files, \
    delete, mkdirs, get_status, DbfsErrorCodes, move
from databricks_cli.dbfs.dbfs_path import DbfsPath, DbfsPathClickType
from databricks_cli.dbfs.exceptions import LocalFileExistsException


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--absolute', is_flag=True, default=False,
              help='Displays absolute paths.')
@click.option('-l', is_flag=True, default=False,
              help='Displays full information including size and file type.')
@click.argument('dbfs_path', nargs=-1, type=DbfsPathClickType())
@require_config
@eat_exceptions
def ls_cli(l, absolute, dbfs_path): #  NOQA
    """
    List files in DBFS.
    """
    if len(dbfs_path) == 0:
        dbfs_path = DbfsPath('dbfs:/')
    elif len(dbfs_path) == 1:
        dbfs_path = dbfs_path[0]
    else:
        error_and_quit('ls can take a maximum of one path.')
    files = list_files(dbfs_path)
    table = tabulate([f.to_row(is_long_form=l, is_absolute=absolute) for f in files],
                     tablefmt='plain')
    click.echo(table)


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('dbfs_path', type=DbfsPathClickType())
@require_config
@eat_exceptions
def mkdirs_cli(dbfs_path):
    """
    Make directories in DBFS.

    Note that this function is subtly different than mkdir. Mkdir requires
    the parent directory of the input to already exist. Mkdirs will create directories
    along the path to the argument directory.
    """
    mkdirs(dbfs_path)


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--recursive', '-r', is_flag=True, default=False)
@click.argument('dbfs_path', type=DbfsPathClickType())
@require_config
@eat_exceptions
def rm_cli(recursive, dbfs_path):
    """
    Remove files from dbfs.

    To remove a directory you must provide the --recursive flag.
    """
    delete(dbfs_path, recursive)


def copy_to_dbfs(src, dbfs_path_dst, overwrite):
    try:
        put_file(src, dbfs_path_dst, overwrite)
    except HTTPError as e:
        if e.response.json()['error_code'] == DbfsErrorCodes.RESOURCE_ALREADY_EXISTS:
            click.echo(e.response.json())
        else:
            raise e


def copy_from_dbfs(dbfs_path_src, dst, overwrite):
    # Will truncate the local file if it exists.
    try:
        get_file(dbfs_path_src, dst, overwrite)
    except LocalFileExistsException:
        click.echo(('The host destination {} already exists. You should provide the '
                   '--overwrite flag.')
                   .format(dst))


def copy_to_dbfs_non_recursive(src, dbfs_path_dst, overwrite):
    # Munge dst path in case dbfs_path_dst is a dir
    try:
        if get_status(dbfs_path_dst).is_dir:
            dbfs_path_dst = dbfs_path_dst.join(os.path.basename(src))
    except HTTPError as e:
        if e.response.json()['error_code'] == DbfsErrorCodes.RESOURCE_DOES_NOT_EXIST:
            pass
        else:
            raise e
    copy_to_dbfs(src, dbfs_path_dst, overwrite)


def copy_from_dbfs_non_recursive(dbfs_path_src, dst, overwrite):
    # Munge dst path in case dst is a dir
    if os.path.isdir(dst):
        dst = os.path.join(dst, dbfs_path_src.basename)
    copy_from_dbfs(dbfs_path_src, dst, overwrite)


def copy_to_dbfs_recursive(src, dbfs_path_dst, overwrite):
    try:
        mkdirs(dbfs_path_dst)
    except HTTPError as e:
        if e.response.json()['error_code'] == DbfsErrorCodes.RESOURCE_ALREADY_EXISTS:
            click.echo(e.response.json())
            return
    for filename in os.listdir(src):
        cur_src = os.path.join(src, filename)
        cur_dbfs_dst = dbfs_path_dst.join(filename)
        if os.path.isdir(cur_src):
            copy_to_dbfs_recursive(cur_src, cur_dbfs_dst, overwrite)
        elif os.path.isfile(cur_src):
            copy_to_dbfs(cur_src, cur_dbfs_dst, overwrite)


def copy_from_dbfs_recursive(dbfs_path_src, dst, overwrite):
    if os.path.isfile(dst):
        click.echo('{} exists as a file. Skipping this subtree {}'.format(dst, repr(dbfs_path_src)))
    elif not os.path.isdir(dst):
        os.makedirs(dst)

    for dbfs_src_file_info in list_files(dbfs_path_src):
        cur_dbfs_src = dbfs_src_file_info.dbfs_path
        cur_dst = os.path.join(dst, cur_dbfs_src.basename)
        if dbfs_src_file_info.is_dir:
            copy_from_dbfs_recursive(cur_dbfs_src, cur_dst, overwrite)
        else:
            copy_from_dbfs(cur_dbfs_src, cur_dst, overwrite)


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--recursive', '-r', is_flag=True, default=False)
@click.option('--overwrite', is_flag=True, default=False)
@click.argument('src')
@click.argument('dst')
@require_config
@eat_exceptions
def cp_cli(recursive, overwrite, src, dst):
    """
    Copy files to and from DBFS.

    Note that this function will fail if the src and dst are both on the local filesystem
    or if they are both DBFS paths.

    For non-recursive copies, if the dst is a directory, the file will be placed inside the
    directory. For example ``dbfs cp dbfs:/apple.txt .`` will create a file at `./apple.txt`.

    For recursive copies, files inside of the src directory will be copied inside the dst directory
    with the same name. If the dst path does not exist, a directory will be created. For example
    ``dbfs cp -r dbfs:/foo foo`` will create a directory foo and place the files ``dbfs:/foo/a`` at
    ``foo/a``. If ``foo/a`` already exists, the file will not be overriden unless the --overwrite
    flag is provided -- however, dbfs cp --recursive will continue to try and copy other files.
    """
    # Copy to DBFS in this case
    if not DbfsPath.is_valid(src) and DbfsPath.is_valid(dst):
        if not os.path.exists(src):
            error_and_quit('The local file {} does not exist.'.format(src))
        if not recursive:
            if os.path.isdir(src):
                error_and_quit(('The local file {} is a directory. You must provide --recursive')
                               .format(src))
            copy_to_dbfs_non_recursive(src, DbfsPath(dst), overwrite)
        else:
            if not os.path.isdir(src):
                copy_to_dbfs_non_recursive(src, DbfsPath(dst), overwrite)
                return
            copy_to_dbfs_recursive(src, DbfsPath(dst), overwrite)
    # Copy from DBFS in this case
    elif DbfsPath.is_valid(src) and not DbfsPath.is_valid(dst):
        if not recursive:
            copy_from_dbfs_non_recursive(DbfsPath(src), dst, overwrite)
        else:
            dbfs_path_src = DbfsPath(src)
            if not get_status(dbfs_path_src).is_dir:
                copy_from_dbfs_non_recursive(dbfs_path_src, dst, overwrite)
            copy_from_dbfs_recursive(dbfs_path_src, dst, overwrite)
    elif not DbfsPath.is_valid(src) and not DbfsPath.is_valid(dst):
        error_and_quit('Both paths provided are from your local filesystem. '
                       'To use this utility, one of the src or dst must be prefixed '
                       'with dbfs:/')
    elif DbfsPath.is_valid(src) and DbfsPath.is_valid(dst):
        error_and_quit('Both paths provided are from the DBFS filesystem. '
                       'To copy between the DBFS filesystem, you currently must copy the '
                       'file from DBFS to your local filesystem and then back.')
    else:
        assert False, 'not reached'


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('src', type=DbfsPathClickType())
@click.argument('dst', type=DbfsPathClickType())
@require_config
@eat_exceptions
def mv_cli(src, dst):
    """
    Moves a file between two DBFS paths.
    """
    move(src, dst)


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True)
def dbfs_group():
    """
    Utility to interact with DBFS.
    DBFS paths are all prefixed with dbfs:/. Local paths can be absolute or local.
    """
    pass


dbfs_group.add_command(configure_cli, name='configure')
dbfs_group.add_command(ls_cli, name='ls')
dbfs_group.add_command(mkdirs_cli, name='mkdirs')
dbfs_group.add_command(rm_cli, name='rm')
dbfs_group.add_command(cp_cli, name='cp')
dbfs_group.add_command(mv_cli, name='mv')
