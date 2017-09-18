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

from databricks_cli.utils import eat_exceptions, CONTEXT_SETTINGS
from databricks_cli.version import print_version_callback
from databricks_cli.configure.config import require_config
from databricks_cli.dbfs.exceptions import LocalFileExistsException
from databricks_cli.workspace.api import list_objects, mkdirs, import_workspace, export_workspace, \
    delete, get_status
from databricks_cli.workspace.types import LanguageClickType, FormatClickType, WorkspaceFormat, \
    WorkspaceLanguage


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--absolute', is_flag=True, default=False,
              help='Displays absolute paths.')
@click.option('-l', is_flag=True, default=False,
              help='Displays full information including ObjectType, Language, Path')
@click.argument('workspace_path', type=str, nargs=-1)
@require_config
@eat_exceptions
def ls_cli(l, absolute, workspace_path):
    """
    List objects in the Databricks Workspace
    """
    if len(workspace_path) == 0:
        workspace_path = '/'
    else:
        workspace_path = workspace_path[0]
    objects = list_objects(workspace_path)
    table = tabulate([obj.to_row(is_long_form=l, is_absolute=absolute) for obj in objects],
                     tablefmt='plain')
    click.echo(table)


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('workspace_path')
@require_config
@eat_exceptions
def mkdirs_cli(workspace_path):
    """
    Make directories in the Databricks Workspace.

    Mkdirs will create directories along the path to the argument directory.
    """
    mkdirs(workspace_path)


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('source_path')
@click.argument('target_path')
@click.option('--language', '-l', required=True, type=LanguageClickType())
@click.option('--format', '-f', default=WorkspaceFormat.SOURCE, type=FormatClickType())
@click.option('--overwrite', '-o', is_flag=True, default=False)
@require_config
@eat_exceptions
def import_workspace_cli(source_path, target_path, language, format, overwrite): # NOQA
    """
    Imports a file from local to the Databricks workspace.

    The format is by default SOURCE.
    """
    import_workspace(source_path, target_path, language, format, overwrite) # NOQA


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('source_path')
@click.argument('target_path')
@click.option('--format', '-f', default=WorkspaceFormat.SOURCE, type=FormatClickType())
@click.option('--overwrite', '-o', is_flag=True, default=False)
@require_config
@eat_exceptions
def export_workspace_cli(source_path, target_path, format, overwrite): # NOQA
    """
    Exports a file from the Databricks workspace to local.

    The format is by default SOURCE.
    """
    export_workspace(source_path, target_path, format, overwrite) # NOQA


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('workspace_path')
@click.option('--recursive', '-r', is_flag=True, default=False)
@require_config
@eat_exceptions
def delete_cli(workspace_path, recursive):
    """
    Deletes objects from the Databricks workspace.

    To delete a folder add the recursive flag.
    """
    delete(workspace_path, recursive)


def _export_dir_helper(source_path, target_path, overwrite):
    if os.path.isfile(target_path):
        click.echo('{} exists as a file. Skipping this subtree {}'
                   .format(target_path, source_path))
        return
    if not os.path.isdir(target_path):
        os.makedirs(target_path)
    for obj in list_objects(source_path):
        cur_src = obj.path
        cur_dst = os.path.join(target_path, obj.basename)
        if obj.is_dir:
            _export_dir_helper(cur_src, cur_dst, overwrite)
        elif obj.is_notebook:
            cur_dst = cur_dst + WorkspaceLanguage.to_extension(obj.language)
            try:
                export_workspace(cur_src, cur_dst, WorkspaceFormat.SOURCE, overwrite)
            except LocalFileExistsException:
                click.echo('{} already exists locally as {}. Skip.'.format(cur_src, cur_dst))
            click.echo('{} -> {}'.format(cur_src, cur_dst))
        else:
            click.echo('{} is neither a dir or a notebook. Skip.'.format(cur_src))


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('source_path')
@click.argument('target_path')
@click.option('--overwrite', '-o', is_flag=True, default=False)
@require_config
@eat_exceptions
def export_dir_cli(source_path, target_path, overwrite):
    """
    Recursively exports a directory from the Databricks workspace to local.

    Only directories and notebooks are exported. Notebooks are always exported in the SOURCE
    format. Notebooks will also have the extension of .scala, .py, .sql, or .r appended
    depending on the language type.
    """
    assert get_status(source_path).is_dir, 'The source path must be a directory. {}' \
        .format(source_path)
    _export_dir_helper(source_path, target_path, overwrite)


def _import_dir_helper(source_path, target_path, overwrite):
    try:
        mkdirs(target_path)
    except HTTPError as e:
        click.echo(e.response.json())
        return
    for filename in os.listdir(source_path):
        cur_src = os.path.join(source_path, filename)
        cur_dst = os.path.join(target_path, filename)
        if os.path.isdir(cur_src):
            _import_dir_helper(cur_src, cur_dst, overwrite)
        elif os.path.isfile(cur_src):
            ext = WorkspaceLanguage.get_extension(cur_src)
            if ext != '':
                cur_dst = cur_dst.rstrip(ext)
                language = WorkspaceLanguage.to_language(cur_src)
                import_workspace(cur_src, cur_dst, language, WorkspaceFormat.SOURCE, overwrite)
                click.echo('{} -> {}'.format(cur_src, cur_dst))
            else:
                extensions = ', '.join(WorkspaceLanguage.EXTENSIONS)
                click.echo(('{} does not have a valid extension of {}. Skip this file and ' +
                            'continue.').format(cur_src, extensions))


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('source_path')
@click.argument('target_path')
@click.option('--overwrite', '-o', is_flag=True, default=False)
@require_config
@eat_exceptions
def import_dir_cli(source_path, target_path, overwrite):
    """
    Recursively imports a directory from local to the Databricks workspace.

    Only directories and files with the extensions .scala, .py, .sql, .r, .R are imported.
    When imported, these extensions will be stripped off the name of the notebook.
    """
    _import_dir_helper(source_path, target_path, overwrite)


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True)
def workspace_group():
    """
    Utility to interact with the Databricks Workspace.
    Workspace paths must be absolute and be prefixed with `/`.
    """
    pass

workspace_group.add_command(ls_cli, name='ls')
workspace_group.add_command(ls_cli, name='list')
workspace_group.add_command(mkdirs_cli, name='mkdirs')
workspace_group.add_command(import_workspace_cli, name='import')
workspace_group.add_command(export_workspace_cli, name='export')
workspace_group.add_command(delete_cli, name='delete')
workspace_group.add_command(delete_cli, name='rm')
workspace_group.add_command(export_dir_cli, name='export_dir')
workspace_group.add_command(import_dir_cli, name='import_dir')
