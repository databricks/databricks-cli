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

import tempfile
import click

from databricks_cli.containers.utils import build_image, export_image, get_dbfs_path, \
    upload_image_to_dbfs, upload_image_to_s3
from databricks_cli.configure.config import provide_api_client, profile_option
from databricks_cli.dbfs.dbfs_path import DbfsPathClickType
from databricks_cli.utils import eat_exceptions, CONTEXT_SETTINGS
from databricks_cli.version import print_version_callback, version


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('dockerfile')
@profile_option
@eat_exceptions
@provide_api_client
def build_export_upload(api_client, dockerfile):
    """
    Builds, exports, and uploads a custom container to DBFS at a random path.
    """
    image_id = build_image(dockerfile)
    with tempfile.NamedTemporaryFile(suffix='.lz4') as temp:
        export_image(image_id, temp.name)
        filename = temp.name.split("/")[-1]
        upload_image_to_dbfs(api_client, temp.name, get_dbfs_path(filename))


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('dockerfile')
def build(dockerfile):
    """
    Builds a custom image with a Dockerfile
    """
    build_image(dockerfile)


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('image_id')
@click.argument('exported_image')
def export(image_id, exported_image):
    """
    Exports and compresses a custom image
    """
    export_image(image_id, exported_image)


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('exported_image')
@click.argument('s3_bucket')
@click.argument('s3_path')
@click.argument('access_key')
@click.argument('secret_key')
def upload_s3(exported_image, s3_bucket, s3_path, access_key, secret_key):
    """
    Uploads and presigns a custom image to s3
    """
    upload_image_to_s3(exported_image, s3_bucket, s3_path, access_key, secret_key)

@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('exported_image')
@click.argument('dbfs_path', default=None, type=DbfsPathClickType())
@profile_option
@eat_exceptions
@provide_api_client
def upload_dbfs(api_client, exported_image, dbfs_path):
    """
    Uploads a custom image to DBFS
    """
    upload_image_to_dbfs(api_client, exported_image, dbfs_path)


@click.group(context_settings=CONTEXT_SETTINGS,
             short_help='[BETA] Utility to build, export, and push Databricks containers.')
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@eat_exceptions
def containers_group():
    """
    [BETA] Utility to build, export, and push Databricks containers
    """
    pass


containers_group.add_command(build, name='build')
containers_group.add_command(build_export_upload, name='build-export-upload')
containers_group.add_command(export, name='export')
containers_group.add_command(upload_dbfs, name='upload-dbfs')
containers_group.add_command(upload_s3, name='upload-s3')
