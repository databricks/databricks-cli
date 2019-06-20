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


from hashlib import sha1
import os

from six.moves.urllib.parse import urlsplit

from databricks_cli.sdk import DeltaPipelinesService
from databricks_cli.dbfs.api import DbfsApi
from databricks_cli.dbfs.dbfs_path import DbfsPath


# These imports are specific to the credentials part
from databricks_cli.configure.config import get_profile_from_context
from databricks_cli.configure.provider import get_config, ProfileConfigProvider
from databricks_cli.utils import InvalidConfigurationError


class PipelinesApi(object):
    def __init__(self, api_client):
        self.client = DeltaPipelinesService(api_client)
        self.dbfs_client = DbfsApi(api_client)

    @staticmethod
    def _partition_libraries_and_extract_local_paths(lib_objects):
        local_lib_objects, rest_lib_objects = [], []
        for lib_object in lib_objects:
            uri_scheme = urlsplit(lib_object.path).scheme
            if lib_object.lib_type == 'jar' and uri_scheme == '':
                local_lib_objects.append(lib_object)
            elif lib_object.lib_type == 'jar' and uri_scheme.lower() == 'file':
                local_lib_objects.append(LibraryObject(lib_object.lib_type, lib_object.path[5:]))
            else:
                rest_lib_objects.append(lib_object)
        return local_lib_objects, rest_lib_objects

    @staticmethod
    def _get_hashed_path(path):
        hash_buffer = sha1()
        BUFFER_SIZE = 1024 * 64
        try:
            with open(path, 'rb') as f:
                while True:
                    data = f.read(BUFFER_SIZE)
                    if not data:
                        break
                    hash_buffer.update(data)
        except Exception as e:
            raise RuntimeError('Error \'{}\' while processing {}'.format(e, path))

        file_hash = hash_buffer.hexdigest()
        path = 'dbfs:/pipelines/code/{}{}'.format(file_hash, os.path.splitext(path)[1])
        return path

    def _get_remote_mappings(self, local_lib_objects):
        return list(map(lambda llo: LibraryObject(llo.lib_type, self._get_hashed_path(llo.path))
                        , local_lib_objects))

    def _get_files_to_upload(self, local_lib_objects, remote_lib_objects):
        transformed_remote_lib_objects = map(lambda rlo: LibraryObject(rlo.lib_type, DbfsPath(rlo.path))
                                             , remote_lib_objects)
        return list(filter(lambda lo_tuple: not self.dbfs_client.file_exists(lo_tuple[1].path)
                           , zip(local_lib_objects, transformed_remote_lib_objects)))


    """
    Only required until the deploy/delete APIs requires the credentials in the body as well as the header.
     Once the API requirement is relaxed, this function can be stripped out and 
     includes for this function removed.
    """
    @staticmethod
    def _get_credentials_for_request():
        profile = get_profile_from_context()
        if profile:
            config = ProfileConfigProvider.get_config(profile)
        else:
            config = get_config()
        if not config or not config.is_valid:
            raise InvalidConfigurationError.for_profile(profile)

        if config.is_valid_with_token:
            return {'token': config.token}
        else:
            return {'user': config.username, 'password': config.password}

    def deploy(self, spec, headers=None):
        lib_objects = LibraryObject.convert_from_libraries(spec.get('libraries', []))
        local_lib_objects, rest_lib_objects = self._partition_libraries_and_extract_local_paths(lib_objects)
        remote_lib_objects = self._get_remote_mappings(local_lib_objects)
        upload_files = self._get_files_to_upload(local_lib_objects, remote_lib_objects)

        for llo, rlo in upload_files:
            try:
                self.dbfs_client.put_file(llo.path, rlo.path, False)
            except Exception as e:
                raise RuntimeError('Error \'{}\' while uploading {}'.format(e, llo.path))

        rest_lib_objects.extend(remote_lib_objects)
        spec['libraries'] = LibraryObject.convert_to_libraries(rest_lib_objects)
        spec['credentials'] = self._get_credentials_for_request()
        self.client.client.perform_query('PUT', '/pipelines/{}'.format(spec['id']), data=spec, headers=headers)

    def delete(self, pipeline_id, headers=None):
        self.client.delete(pipeline_id, self._get_credentials_for_request(), headers)


class LibraryObject(object):
    def __init__(self, lib_type, lib_path):
        self.path = lib_path
        self.lib_type = lib_type

    @classmethod
    def convert_from_libraries(cls, libraries):
        lib_objects = []
        for library in libraries:
            for lib_type, path in library.items():
                lib_objects.append(LibraryObject(lib_type, path))
        return lib_objects

    @classmethod
    def convert_to_libraries(cls, lib_objects):
        libraries = []
        for lib_object in lib_objects:
            libraries.append({lib_object.lib_type: lib_object.path})
        return libraries
