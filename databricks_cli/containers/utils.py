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

import datetime
import os
import subprocess
import tempfile

import boto3
from botocore.client import Config
import lz4.frame

from click import ClickException

from databricks_cli.dbfs.api import DbfsApi
from databricks_cli.dbfs.cli import copy_to_dbfs_non_recursive
from databricks_cli.dbfs.dbfs_path import DbfsPath

# This is the only DBR version we support in the MVP of BYOC
DBR = "databricks/4.0.x-scala2.11"


def get_dbfs_path(name):
    """
    Given the file name of the compressed tarball, generate a dbfs path to upload to.
    """
    return DbfsPath("dbfs:/containers/{0}-{1}".format(datetime.datetime.now(), name))


def build_image(dockerfile):
    """
    Calls the docker client to build the custom image. This will stream the build output to STDOUT.
    Assumes that a docker daemon is running and the `docker` command is available.
    :return: the image id of the custom image
    """
    validate_dockerfile(dockerfile)
    print("Building custom image with dockerfile: {}".format(dockerfile))
    dockerfile_folder = os.path.dirname(dockerfile)
    docker_build_command = ["docker", "build", dockerfile_folder]
    image_id = run_cmd(docker_build_command)
    print("Built custom image with id: {}".format(image_id))
    return image_id


def export_image(image_id, exported_image):
    """
    Given the docker image id, this will create a compressed tarball that Databricks can launch.
    Specifically, this will:
        1. Create a container from the image (`docker create <IMAGE_ID>`)
        2. Export the container's filesystem (`docker export <CONTAINER_ID>`)
        3. Compresses the filesystem tarball (`lz4 -1 <CONTAINER_TARBALL>`)
    """
    validate_exported_image(exported_image)
    print "Starting image export of image {0}. Docker container id:".format(image_id)
    container_id = run_cmd(["docker", "create", image_id])

    with open(exported_image, 'w') as output:
        c_context = lz4.frame.create_compression_context()
        output.write(lz4.frame.compress_begin(c_context))
        export_proc = subprocess.Popen(["docker", "export", container_id], stdout=subprocess.PIPE)
        for line in export_proc.stdout:
            output.write(lz4.frame.compress_chunk(c_context, line))
        output.write(lz4.frame.compress_flush(c_context))

    print "Finished image export to compressed tarball {}".format(exported_image)


def upload_image_to_dbfs(api_client, exported_image, dbfs_path=None):
    """
    Uploads the compressed exported custom image to DBFS, given the current CLI user's context.
    :param: dbfs_path if None, this will generate an unique DBFS path to upload to
    :return: the path of the uploaded custom image
    """
    validate_exported_image(exported_image)
    dbfs_api = DbfsApi(api_client)
    if dbfs_path is None:
        dbfs_path = get_dbfs_path(exported_image)
    else:
        validate_dbfs_path(dbfs_path.absolute_path)

    print('Uploading custom image from "{0}" to DBFS at: {1}'.format(exported_image, dbfs_path))
    copy_to_dbfs_non_recursive(dbfs_api, exported_image, dbfs_path, False)
    print('Uploaded custom image to DBFS at: {}'.format(dbfs_path))
    return dbfs_path


def upload_image_to_s3(exported_image, s3_bucket, s3_path, access_key, secret_key):
    """
    This will upload the compressed exported custom image to a specified s3 bucket, and then
    using the same credentials presign the image.
    """
    validate_exported_image(exported_image)
    validate_s3_path(s3_path)
    print('Uploading custom image to s3://{0}/{1}'.format(s3_bucket, s3_path))
    s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key,
                             config=Config(signature_version='s3'))
    s3_client.upload_file(exported_image, s3_bucket, s3_path)
    url = s3_client.generate_presigned_url(
        ClientMethod='get_object',
        Params={
            'Bucket': s3_bucket,
            'Key': s3_path,
        },
        # 1 Year in Seconds
        ExpiresIn=31536000,
    )
    print('Presigned url: {}'.format(url))


### Helper methods

def run_cmd(command):
    """
    Runs a command, streams output to stdout and to a file, and returns the last token of the
    output as a string.
    """
    command_string = ' '.join(command)
    with tempfile.NamedTemporaryFile() as temp:
        piped_command = ["bash", "-c", '{0} | tee {1}'.format(command_string, temp.name)]
        subprocess.check_call(piped_command, stderr=subprocess.STDOUT)
        output = get_file_content(temp.name)
        if len(output) == 0:
            raise ClickException('Command "{}" failed!'.format(command_string))
        return output.strip().split()[-1]


def validate_exported_image(exported_image):
    error_message = 'Exported image "{}" should have a ".lz4" suffix.'.format(exported_image)
    validate_lz4_extension(exported_image, error_message)


def validate_dbfs_path(dbfs_path):
    error_message = 'DBFS path "{}" should have a ".lz4" suffix.'.format(dbfs_path)
    validate_lz4_extension(dbfs_path, error_message)


def validate_s3_path(s3_path):
    error_message = 'S3 path "{}" should have a ".lz4" suffix.'.format(s3_path)
    validate_lz4_extension(s3_path, error_message)


def validate_lz4_extension(name, error_message):
    if not name.strip().endswith(".lz4"):
        raise ClickException(error_message)


FIRST_LINE_OF_DOCKERFILE = "FROM {}".format(DBR)


def validate_dockerfile(dockerfile):
    """
    Validates that the docker file starts with "FROM databricks/4.0.x-scala2.11"
    """
    content = get_file_content(dockerfile)
    if not content.strip().startswith(FIRST_LINE_OF_DOCKERFILE):
        raise ClickException('The first line of the dockerfile should start with:\n' +
                             FIRST_LINE_OF_DOCKERFILE)


def get_file_content(filename):
    with open(filename, 'r') as f:
        return f.read()
