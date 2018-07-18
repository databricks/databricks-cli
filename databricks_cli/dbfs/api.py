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

from databricks_cli.sdk import DbfsService
from databricks_cli.utils import error_and_quit
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


class DbfsApi(object):
    def __init__(self, api_client):
        self.client = DbfsService(api_client)

    def list_files(self, dbfs_path):
        list_response = self.client.list(dbfs_path.absolute_path)
        if 'files' in list_response:
            return [FileInfo.from_json(f) for f in list_response['files']]
        else:
            return []

    def file_exists(self, dbfs_path):
        try:
            self.get_status(dbfs_path)
        except HTTPError as e:
            if e.response.json()['error_code'] == DbfsErrorCodes.RESOURCE_DOES_NOT_EXIST:
                return False
            raise e
        return True

    def get_status(self, dbfs_path):
        json = self.client.get_status(dbfs_path.absolute_path)
        return FileInfo.from_json(json)

    def put_file(self, src_path, dbfs_path, overwrite):
        handle = self.client.create(dbfs_path.absolute_path, overwrite)['handle']
        with open(src_path, 'rb') as local_file:
            while True:
                contents = local_file.read(BUFFER_SIZE_BYTES)
                if len(contents) == 0:
                    break
                # add_block should not take a bytes object.
                self.client.add_block(handle, b64encode(contents).decode())
            self.client.close(handle)

    def get_file(self, dbfs_path, dst_path, overwrite):
        if os.path.exists(dst_path) and not overwrite:
            raise LocalFileExistsException('{} exists already.'.format(dst_path))
        file_info = self.get_status(dbfs_path)
        if file_info.is_dir:
            error_and_quit(('The dbfs file {} is a directory.').format(repr(dbfs_path)))
        length = file_info.file_size
        offset = 0
        with open(dst_path, 'wb') as local_file:
            while offset < length:
                response = self.client.read(dbfs_path.absolute_path, offset, BUFFER_SIZE_BYTES)
                bytes_read = response['bytes_read']
                data = response['data']
                offset += bytes_read
                local_file.write(b64decode(data))

    def delete(self, dbfs_path, recursive):
        self.client.delete(dbfs_path.absolute_path, recursive=recursive)

    def mkdirs(self, dbfs_path):
        self.client.mkdirs(dbfs_path.absolute_path)

    def move(self, dbfs_src, dbfs_dst):
        self.client.move(dbfs_src.absolute_path, dbfs_dst.absolute_path)

    def _copy_to_dbfs_non_recursive(self, src, dbfs_path_dst, overwrite):
        # Munge dst path in case dbfs_path_dst is a dir
        try:
            if self.get_status(dbfs_path_dst).is_dir:
                dbfs_path_dst = dbfs_path_dst.join(os.path.basename(src))
        except HTTPError as e:
            if e.response.json()['error_code'] == DbfsErrorCodes.RESOURCE_DOES_NOT_EXIST:
                pass
            else:
                raise e
        self.put_file(src, dbfs_path_dst, overwrite)

    def _copy_from_dbfs_non_recursive(self, dbfs_path_src, dst, overwrite):
        # Munge dst path in case dst is a dir
        if os.path.isdir(dst):
            dst = os.path.join(dst, dbfs_path_src.basename)
        self.get_file(dbfs_path_src, dst, overwrite)

    def _copy_to_dbfs_recursive(self, src, dbfs_path_dst, overwrite):
        try:
            self.mkdirs(dbfs_path_dst)
        except HTTPError as e:
            if e.response.json()['error_code'] == DbfsErrorCodes.RESOURCE_ALREADY_EXISTS:
                click.echo(e.response.json())
                return
        for filename in os.listdir(src):
            cur_src = os.path.join(src, filename)
            cur_dbfs_dst = dbfs_path_dst.join(filename)
            if os.path.isdir(cur_src):
                self._copy_to_dbfs_recursive(cur_src, cur_dbfs_dst, overwrite)
            elif os.path.isfile(cur_src):
                try:
                    self.put_file(cur_src, cur_dbfs_dst, overwrite)
                    click.echo('{} -> {}'.format(cur_src, cur_dbfs_dst))
                except HTTPError as e:
                    if e.response.json()['error_code'] == DbfsErrorCodes.RESOURCE_ALREADY_EXISTS:
                        click.echo('{} already exists. Skip.'.format(cur_dbfs_dst))
                    else:
                        raise e

    def _copy_from_dbfs_recursive(self, dbfs_path_src, dst, overwrite):
        if os.path.isfile(dst):
            click.echo(
                '{} exists as a file. Skipping this subtree {}'.format(dst, repr(dbfs_path_src)))
            return
        elif not os.path.isdir(dst):
            os.makedirs(dst)

        for dbfs_src_file_info in self.list_files(dbfs_path_src):
            cur_dbfs_src = dbfs_src_file_info.dbfs_path
            cur_dst = os.path.join(dst, cur_dbfs_src.basename)
            if dbfs_src_file_info.is_dir:
                self._copy_from_dbfs_recursive(cur_dbfs_src, cur_dst, overwrite)
            else:
                try:
                    self.get_file(cur_dbfs_src, cur_dst, overwrite)
                    click.echo('{} -> {}'.format(cur_dbfs_src, cur_dst))
                except LocalFileExistsException:
                    click.echo(('{} already exists locally as {}. Skip. To overwrite, you' +
                                'should provide the --overwrite flag.').format(cur_dbfs_src,
                                                                               cur_dst))

    def cp(self, recursive, overwrite, src, dst):
        if not DbfsPath.is_valid(src) and DbfsPath.is_valid(dst):
            if not os.path.exists(src):
                error_and_quit('The local file {} does not exist.'.format(src))
            if not recursive:
                if os.path.isdir(src):
                    error_and_quit(
                        ('The local file {} is a directory. You must provide --recursive')
                        .format(src))
                self._copy_to_dbfs_non_recursive(src, DbfsPath(dst), overwrite)
            else:
                if not os.path.isdir(src):
                    self._copy_to_dbfs_non_recursive(src, DbfsPath(dst), overwrite)
                    return
                self._copy_to_dbfs_recursive(src, DbfsPath(dst), overwrite)
        # Copy from DBFS in this case
        elif DbfsPath.is_valid(src) and not DbfsPath.is_valid(dst):
            if not recursive:
                self._copy_from_dbfs_non_recursive(DbfsPath(src), dst, overwrite)
            else:
                dbfs_path_src = DbfsPath(src)
                if not self.get_status(dbfs_path_src).is_dir:
                    self._copy_from_dbfs_non_recursive(dbfs_path_src, dst, overwrite)
                self._copy_from_dbfs_recursive(dbfs_path_src, dst, overwrite)
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
