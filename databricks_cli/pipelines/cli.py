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
import os

import click

from databricks_cli.click_types import PipelineSpecClickType, PipelineIdClickType
from databricks_cli.utils import eat_exceptions, CONTEXT_SETTINGS, pretty_format
from databricks_cli.version import print_version_callback, version
from databricks_cli.pipelines.api import PipelinesApi
from databricks_cli.configure.config import provide_api_client, profile_option, debug_option


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Deploys a delta pipeline according to the pipeline specification')
@click.argument('spec_arg', default=None, required=False)
@click.option('--spec', default=None, type=PipelineSpecClickType(), help=PipelineSpecClickType.help)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def deploy_cli(api_client, spec_arg, spec):
    """
    Deploys a delta pipeline according to the pipeline specification. The pipeline spec is a
    specification that explains how to run a Delta Pipeline on Databricks. All local libraries
    referenced in the spec are uploaded to DBFS.

    Usage:

    databricks pipelines deploy example.json

    OR

    databricks pipelines deploy --spec example.json
    """
    if bool(spec_arg) == bool(spec):
        raise RuntimeError('The spec should be provided either by an option or argument')
    src = spec_arg if bool(spec_arg) else spec
    spec_obj = _read_spec(src)
    PipelinesApi(api_client).deploy(spec_obj)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Stops a delta pipeline and deletes its associated Databricks resources')
@click.argument('spec_arg', default=None, required=False)
@click.option('--spec', default=None, type=PipelineSpecClickType(), help=PipelineSpecClickType.help)
@click.option('--pipeline-id', default=None, type=PipelineIdClickType(),
              help=PipelineIdClickType.help)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def delete_cli(api_client, spec_arg, spec, pipeline_id):
    """
    Stops a delta pipeline and deletes its associated Databricks resources. The pipeline can be
    resumed by deploying it again.

    Usage:

    databricks pipelines delete example.json

    OR

    databricks pipelines delete --spec example.json

    OR

    databricks pipelines delete --pipeline-id 1234
    """
    pipeline_id = _get_pipeline_id(spec_arg=spec_arg, spec=spec, pipeline_id=pipeline_id)
    PipelinesApi(api_client).delete(pipeline_id)


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Gets a delta pipeline\'s current spec and status')
@click.argument('spec_arg', default=None, required=False)
@click.option('--spec', default=None, type=PipelineSpecClickType(), help=PipelineSpecClickType.help)
@click.option('--pipeline-id', default=None, type=PipelineIdClickType(),
              help=PipelineIdClickType.help)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def get_cli(api_client, spec_arg, spec, pipeline_id):
    """
    Gets a delta pipeline's current spec and status.

    Usage:

    databricks pipelines get example.json

    OR

    databricks pipelines get --spec example.json

    OR

    databricks pipelines get --pipeline-id 1234
    """
    pipeline_id = _get_pipeline_id(spec_arg=spec_arg, spec=spec, pipeline_id=pipeline_id)
    click.echo(pretty_format(PipelinesApi(api_client).get(pipeline_id)))


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Resets a delta pipeline so data can be reprocessed from scratch')
@click.argument('spec_arg', default=None, required=False)
@click.option('--spec', default=None, type=PipelineSpecClickType(), help=PipelineSpecClickType.help)
@click.option('--pipeline-id', default=None, type=PipelineIdClickType(),
              help=PipelineIdClickType.help)
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def reset_cli(api_client, spec_arg, spec, pipeline_id):
    """
    Resets a delta pipeline by truncating tables and creating new checkpoint folders so data is
    reprocessed from scratch.

    Usage:

    databricks pipelines reset example.json

    OR

    databricks pipelines reset --spec example.json

    OR

    databricks pipelines reset --pipeline-id 1234
    """
    pipeline_id = _get_pipeline_id(spec_arg=spec_arg, spec=spec, pipeline_id=pipeline_id)
    PipelinesApi(api_client).reset(pipeline_id)


def _read_spec(src):
    """
    Reads the spec at src as a JSON if no file extension is provided, or if in the extension format
    if the format is supported.
    """
    extension = os.path.splitext(src)[1]
    if extension.lower() == '.json':
        with open(src, 'r') as f:
            json = f.read()
        return json_loads(json)
    else:
        raise RuntimeError('The provided file extension for the spec is not supported')


def _get_pipeline_id(spec_arg, spec, pipeline_id):
    """
    Ensures that the user has either specified a spec (either through argument or option) or a
    pipeline ID directly, and returns the pipeline id to use.
    """
    # Only one out of spec/pipeline_id/spec_arg should be supplied
    if bool(spec_arg) + bool(spec) + bool(pipeline_id) != 1:
        raise RuntimeError('Either spec should be provided as an argument '
                           'or option, or the pipeline-id should be provided')
    if bool(spec_arg) or bool(spec):
        src = spec_arg if bool(spec_arg) else spec
        pipeline_id = _read_spec(src)["id"]
    return pipeline_id


@click.group(context_settings=CONTEXT_SETTINGS,
             short_help='Utility to interact with the Databricks Delta Pipelines.')
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@debug_option
@profile_option
def pipelines_group():
    """
    Utility to interact with the Databricks pipelines.
    """
    pass


pipelines_group.add_command(deploy_cli, name='deploy')
pipelines_group.add_command(delete_cli, name='delete')
pipelines_group.add_command(get_cli, name='get')
pipelines_group.add_command(reset_cli, name='reset')
