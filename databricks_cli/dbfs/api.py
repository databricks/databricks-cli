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

from base64 import b64encode, b64decode

import os
import click

from requests.exceptions import HTTPError
from databricks_cli.utils import error_and_quit
from databricks_cli.configure.config import get_dbfs_client
from databricks_cli.dbfs.dbfs_path import DbfsPath
from databricks_cli.dbfs.exceptions import LocalFileExistsException

BUFFER_SIZE_BYTES = 2**20


class FileInfo(object):
    def __init__(self, dbfs_path, is_dir, file_size):
        self.dbfs_path = dbfs_path
        self.is_dir = is_dir
        self.file_size = file_size

    def to_row(self, is_long_form, is_absolute):
        path = self.dbfs_path.absolute_path if is_absolute else self.dbfs_path.basename
        stylized_path = click.style(path, 'cyan') if self.is_dir else path
        if is_long_form:
            filetype = 'dir' if self.is_dir else 'file'
            return [filetype, self.file_size, stylized_path]
        return [stylized_path]

    @classmethod
    def from_json(cls, json):
        dbfs_path = DbfsPath.from_api_path(json['path'])
        return cls(dbfs_path, json['is_dir'], json['file_size'])

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.dbfs_path == other.dbfs_path and \
                self.is_dir == other.is_dir and \
                self.file_size == other.file_size
        return False


class DbfsErrorCodes(object):
    RESOURCE_DOES_NOT_EXIST = 'RESOURCE_DOES_NOT_EXIST'
    RESOURCE_ALREADY_EXISTS = 'RESOURCE_ALREADY_EXISTS'


def list_files(dbfs_path):
    dbfs_api = get_dbfs_client()
    list_response = dbfs_api.list(dbfs_path.absolute_path)
    if 'files' in list_response:
        return [FileInfo.from_json(f) for f in list_response['files']]
    else:
        return []


def file_exists(dbfs_path):
    try:
        get_status(dbfs_path)
    except HTTPError as e:
        if e.response.json()['error_code'] == DbfsErrorCodes.RESOURCE_DOES_NOT_EXIST:
            return False
        raise e
    return True


def get_status(dbfs_path):
    dbfs_api = get_dbfs_client()
    json = dbfs_api.get_status(dbfs_path.absolute_path)
    return FileInfo.from_json(json)


def put_file(src_path, dbfs_path, overwrite):
    dbfs_api = get_dbfs_client()
    handle = dbfs_api.create(dbfs_path.absolute_path, overwrite)['handle']
    with open(src_path, 'rb') as local_file:
        while True:
            contents = local_file.read(BUFFER_SIZE_BYTES)
            if len(contents) == 0:
                break
            dbfs_api.add_block(handle, b64encode(contents))
        dbfs_api.close(handle)


def get_file(dbfs_path, dst_path, overwrite):
    if os.path.exists(dst_path) and not overwrite:
        raise LocalFileExistsException
    dbfs_api = get_dbfs_client()
    file_info = get_status(dbfs_path)
    if file_info.is_dir:
        error_and_quit(('The dbfs file {} is a directory.').format(repr(dbfs_path)))
    length = file_info.file_size
    offset = 0
    with open(dst_path, 'wb') as local_file:
        while offset < length:
            response = dbfs_api.read(dbfs_path.absolute_path, offset, BUFFER_SIZE_BYTES)
            bytes_read = response['bytes_read']
            data = response['data']
            offset += bytes_read
            local_file.write(b64decode(data))


def delete(dbfs_path, recursive):
    dbfs_api = get_dbfs_client()
    dbfs_api.delete(dbfs_path.absolute_path, recursive=recursive)


def mkdirs(dbfs_path):
    dbfs_api = get_dbfs_client()
    dbfs_api.mkdirs(dbfs_path.absolute_path)


def move(dbfs_src, dbfs_dst):
    dbfs_api = get_dbfs_client()
    dbfs_api.move(dbfs_src.absolute_path, dbfs_dst.absolute_path)
