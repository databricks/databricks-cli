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
import json
import string
import requests

try:
    from urlparse import urlparse, urljoin
except ImportError:
    from urllib.parse import urlparse, urljoin

import click

from databricks_cli.click_types import PipelineSpecClickType, PipelineIdClickType
from databricks_cli.version import print_version_callback, version
from databricks_cli.pipelines.api import PipelinesApi
from databricks_cli.configure.config import provide_api_client, profile_option, debug_option
from databricks_cli.utils import pipelines_exception_eater, CONTEXT_SETTINGS, pretty_format, \
    error_and_quit

try:
    json_parse_exception = json.decoder.JSONDecodeError
except AttributeError:  # Python 2
    json_parse_exception = ValueError

PIPELINE_ID_PERMITTED_CHARACTERS = set(string.ascii_letters + string.digits + '-_')


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Deploys a pipeline according to the pipeline specification.')
@click.argument('spec_arg', default=None, required=False)
@click.option('--spec', default=None, type=PipelineSpecClickType(), help=PipelineSpecClickType.help)
@click.option('--allow-duplicate-names', is_flag=True,
              help="If true, skips duplicate name checking while deploying the pipeline.")
@click.option('--pipeline-id', default=None, type=PipelineIdClickType(),
              help=PipelineIdClickType.help)
@debug_option
@profile_option
@pipelines_exception_eater
@provide_api_client
def deploy_cli(api_client, spec_arg, spec, allow_duplicate_names, pipeline_id):
    """
    Deploys a pipeline according to the pipeline specification. The pipeline spec is a
    JSON document that defines the required settings to run a Delta Live Tables pipeline
    on Databricks. All local libraries referenced in the spec are uploaded to DBFS.

    If the pipeline spec contains an "id" field, or if a pipeline ID is specified directly
    (using the  --pipeline-id argument), attempts to update an existing pipeline
    with that ID. If it does not, creates a new pipeline and logs the ID of the new pipeline
    to STDOUT. Note that if an ID is both specified in the spec and passed via --pipeline-id,
    the two IDs must be the same, or the command will fail.

    The deploy command will not create a new pipeline if a pipeline with the same name already
    exists. This check can be disabled by adding the --allow-duplicate-names option.

    Usage:

    databricks pipelines deploy example.json

    OR

    databricks pipelines deploy --spec example.json

    OR

    databricks pipelines deploy --pipeline-id 1234 --spec example.json
    """
    if bool(spec_arg) == bool(spec):
        raise ValueError('The spec should be provided either by an option or argument')
    src = spec_arg if bool(spec_arg) else spec
    spec_obj = _read_spec(src)
    spec_dir = os.path.dirname(src)
    if not pipeline_id and 'id' not in spec_obj:
        try:
            response = PipelinesApi(api_client).create(spec_obj, spec_dir, allow_duplicate_names)
        except requests.exceptions.HTTPError as e:
            _handle_duplicate_name_exception(spec_obj, e)

        new_pipeline_id = response['pipeline_id']
        click.echo("Pipeline has been assigned ID {}".format(new_pipeline_id))
        click.echo("Successfully created pipeline: {}".format(
            _get_pipeline_url(api_client, new_pipeline_id)))
        click.echo(new_pipeline_id, err=True)
    else:
        if (pipeline_id and 'id' in spec_obj) and pipeline_id != spec_obj["id"]:
            raise ValueError(
                "The ID provided in --pipeline_id '{}' is different from the ID provided "
                "in the spec '{}'. Resolve the conflict and try the command again. "
                "Because pipeline IDs are no longer persisted after being deleted, Databricks "
                "recommends removing the ID field from your spec."
                .format(pipeline_id, spec_obj["id"])
            )

        spec_obj['id'] = pipeline_id or spec_obj.get('id', None)
        _validate_pipeline_id(spec_obj['id'])

        try:
            PipelinesApi(api_client).deploy(spec_obj, spec_dir, allow_duplicate_names)
        except requests.exceptions.HTTPError as e:
            _handle_duplicate_name_exception(spec_obj, e)
        click.echo("Successfully deployed pipeline: {}".format(
            _get_pipeline_url(api_client, spec_obj['id'])))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Stops the pipeline by cancelling any active update and deletes it.')
@click.option('--pipeline-id', default=None, type=PipelineIdClickType(),
              help=PipelineIdClickType.help)
@debug_option
@profile_option
@pipelines_exception_eater
@provide_api_client
def delete_cli(api_client, pipeline_id):
    """
    Stops the pipeline by cancelling any active update and deletes it.

    Usage:

    databricks pipelines delete --pipeline-id 1234
    """
    _validate_pipeline_id(pipeline_id)
    PipelinesApi(api_client).delete(pipeline_id)
    click.echo("Pipeline {} deleted".format(pipeline_id))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Gets a pipeline\'s current spec and status.')
@click.option('--pipeline-id', default=None, type=PipelineIdClickType(),
              help=PipelineIdClickType.help)
@debug_option
@profile_option
@pipelines_exception_eater
@provide_api_client
def get_cli(api_client, pipeline_id):
    """
    Gets a pipeline's current spec and status.

    Usage:

    databricks pipelines get --pipeline-id 1234
    """
    _validate_pipeline_id(pipeline_id)
    click.echo(pretty_format(PipelinesApi(api_client).get(pipeline_id)))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Gets a pipeline\'s current spec and status.')
@debug_option
@profile_option
@pipelines_exception_eater
@provide_api_client
def list_cli(api_client):
    click.echo(pretty_format(PipelinesApi(api_client).list()))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='[Deprecated] Use the "start --full-refresh" command instead. ' +
                          'Resets a pipeline so that data can be reprocessed ' +
                          'from the beginning.')
@click.option('--pipeline-id', default=None, type=PipelineIdClickType(),
              help=PipelineIdClickType.help)
@debug_option
@profile_option
@pipelines_exception_eater
@provide_api_client
def reset_cli(api_client, pipeline_id):
    """
    [Deprecated] Use the "start --full-refresh" command instead.

    Resets a pipeline by truncating tables and creating new checkpoint folders so that data is
    reprocessed from the beginning.

    Usage:

    databricks pipelines reset --pipeline-id 1234
    """
    click.echo("DeprecationWarning: the \"reset\" command is deprecated, " +
               "use the \"start --full-refresh\" command instead.")
    _validate_pipeline_id(pipeline_id)
    resp = PipelinesApi(api_client).start_update(pipeline_id, full_refresh=True)
    click.echo(_gen_start_update_msg(resp, pipeline_id, True))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='[Deprecated] Use the "start" command instead. ' +
                          'Starts a pipeline update.')
@click.option('--pipeline-id', default=None, type=PipelineIdClickType(),
              help=PipelineIdClickType.help)
@debug_option
@profile_option
@pipelines_exception_eater
@provide_api_client
def run_cli(api_client, pipeline_id):
    """
    [Deprecated] Use the "start" command instead.

    Starts a pipeline update.

    Usage:

    databricks pipelines run --pipeline-id 1234
    """
    click.echo("Deprecation warning: the \"run\" command is deprecated." +
               " Use the \"start\" command instead.")
    _validate_pipeline_id(pipeline_id)
    resp = PipelinesApi(api_client).start_update(pipeline_id, full_refresh=False)
    click.echo(_gen_start_update_msg(resp, pipeline_id, False))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Starts a pipeline update.')
@click.option('--pipeline-id', default=None, type=PipelineIdClickType(),
              help=PipelineIdClickType.help)
@click.option('--full-refresh', default=False, type=bool,
              help='If true, truncates tables and creates new checkpoint' +
                   ' folders so that data is reprocessed from the beginning.')
@debug_option
@profile_option
@pipelines_exception_eater
@provide_api_client
def start_cli(api_client, pipeline_id, full_refresh):
    """
    Starts a pipeline update.

    Usage:

    databricks pipelines start --pipeline-id 1234 --full-refresh=true
    """
    _validate_pipeline_id(pipeline_id)
    resp = PipelinesApi(api_client).start_update(pipeline_id, full_refresh=full_refresh)
    click.echo(_gen_start_update_msg(resp, pipeline_id, full_refresh))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Stops the pipeline by cancelling any active update.')
@click.option('--pipeline-id', default=None, type=PipelineIdClickType(),
              help=PipelineIdClickType.help)
@debug_option
@profile_option
@pipelines_exception_eater
@provide_api_client
def stop_cli(api_client, pipeline_id):
    """
    Stops the pipeline by cancelling any active update.

    Usage:

    databricks pipelines stop --pipeline-id 1234
    """
    _validate_pipeline_id(pipeline_id)
    PipelinesApi(api_client).stop(pipeline_id)
    click.echo("Stopped pipeline {}.".format(pipeline_id))


def _gen_start_update_msg(resp, pipeline_id, full_refresh):
    output_msg = "Started an update "
    if resp and 'update_id' in resp:
        output_msg += "{} ".format(resp.get('update_id'))

    if full_refresh:
        output_msg += "with full refresh "

    output_msg += "for pipeline {}.".format(pipeline_id)
    return output_msg


def _read_spec(src):
    """
    Reads the spec at src as a JSON if no file extension is provided, or if in the extension format
    if the format is supported.
    """
    extension = os.path.splitext(src)[1]
    if extension.lower() == '.json':
        try:
            with open(src, 'r') as f:
                data = f.read()
            return json.loads(data)
        except json_parse_exception as e:
            error_and_quit("Invalid JSON provided in spec\n{}".format(e))
    else:
        raise ValueError('The provided file extension for the spec is not supported. ' +
                         'Only JSON files are supported.')


def _get_pipeline_url(api_client, pipeline_id):
    base_url = "{0.scheme}://{0.netloc}/".format(urlparse(api_client.url))
    return urljoin(base_url, "#joblist/pipelines/{}".format(pipeline_id))


def _write_spec(src, spec):
    """
    Writes the spec at src as JSON.
    """
    data = json.dumps(spec, indent=2) + '\n'
    with open(src, 'w') as f:
        f.write(data)


def _validate_pipeline_id(pipeline_id):
    """
    Checks if the pipeline ID is not empty and contains only hyphen (-),
    underscore (_), and alphanumeric characters.
    """
    if pipeline_id is None or len(pipeline_id) == 0:
        error_and_quit(u'Empty pipeline ID provided')
    if not set(pipeline_id) <= PIPELINE_ID_PERMITTED_CHARACTERS:
        message = u'Pipeline ID {} has invalid character(s)\n'.format(pipeline_id)
        message += u'Valid characters are: _ - a-z A-Z 0-9'
        error_and_quit(message)


def _handle_duplicate_name_exception(spec, exception):
    error_code = None
    try:
        error_code = json.loads(exception.response.text).get('error_code')
    except ValueError:
        pass

    if error_code == 'RESOURCE_CONFLICT':
        raise ValueError("Pipeline with name '{}' already exists. ".format(spec['name']) +
                         "If you are updating an existing pipeline, provide the pipeline " +
                         "ID using --pipeline-id. Otherwise, " +
                         "you can use the --allow-duplicate-names option to skip this check. ")
    raise exception


@click.group(context_settings=CONTEXT_SETTINGS,
             short_help='Utility to interact with the Databricks Delta Pipelines.')
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@debug_option
@profile_option
def pipelines_group():  # pragma: no cover
    """
    Utility to interact with the Databricks pipelines.
    """
    pass


pipelines_group.add_command(deploy_cli, name='deploy')
pipelines_group.add_command(delete_cli, name='delete')
pipelines_group.add_command(get_cli, name='get')
pipelines_group.add_command(list_cli, name='list')
pipelines_group.add_command(start_cli, name='start')
pipelines_group.add_command(stop_cli, name='stop')

# DEPRECATED and will be removed in future versions.
pipelines_group.add_command(reset_cli, name='reset')
pipelines_group.add_command(run_cli, name='run')
