# Databricks CLI
# Copyright 2020 Databricks, Inc.
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

from setuptools import Command

from databricks_cli.dbfs.api import DbfsApi
from databricks_cli.dbfs.dbfs_path import DbfsPath
from databricks_cli.configure.provider import get_config, ProfileConfigProvider
from databricks_cli.configure.config import _get_api_client
from databricks_cli.utils import InvalidConfigurationError

import databricks_cli.sdk.service as service


class InstallLibraryCommand(Command):
    user_options = [
        ('dbfs-path=', None, "Path of a library starting with dbfs://",
                             "default: dbfs:/FileStore/jars/{package_name}"),
        ('cluster-id=', None, "cluster id to distribute it", None),
        ('cluster-tag=', None, "cluster tag to install library", None),
        ('cluster-name=', None, "cluster name to distribute it", None),
        ('databricks-cli-profile=', None, "Databricks CLI profile name", None),
    ]

    def initialize_options(self):
        """Abstract method that is required to be overwritten"""
        self.dbfs_path = None
        self.cluster_id = None
        self.cluster_name = None
        self.cluster_tag = None
        self.databricks_cli_profile = None

    def finalize_options(self):
        """Abstract method that is required to be overwritten"""
        if not self.dbfs_path:
            package_name = self.distribution.get_name()
            self.dbfs_path = f"dbfs:/FileStore/jars/{package_name}"
        if not (self.cluster_id or self.cluster_name or self.cluster_tag):
            raise RuntimeError('One of --cluster-id, --cluster-tag or --cluster-name should be provided')

    def _configure_api(self):
        config = ProfileConfigProvider(
            self.databricks_cli_profile
        ).get_config() if self.databricks_cli_profile else get_config()
        if not config or not config.is_valid:
            raise InvalidConfigurationError.for_profile(
                self.databricks_cli_profile, cli_tool='databricks')
        return _get_api_client(config, "upload_library")

    def _upload_library(self, wheel_file):
        from os.path import basename
        dbfs = DbfsApi(self._configure_api())
        artifact = f'{self.dbfs_path}/{basename(wheel_file)}'
        # TODO: iterate through previous versions & re-link to *-latest.wheel
        dbfs.put_file(wheel_file, DbfsPath(artifact, validate=False), True)
        return artifact

    def _install_library(self, artifact):
        api_client = self._configure_api()
        if self.cluster_tag:
            raise RuntimeError('not yet supported')
        if self.cluster_name:
            raise RuntimeError('not yet supported')
        cluster_id = self.cluster_id
        libraries = service.ManagedLibraryService(api_client)
        libraries.install_libraries(cluster_id, {'whl': artifact})
        # TODO: wait and check cluster status for library to be installed

    def run(self):
        self.run_command('bdist_wheel')
        if not self.distribution.dist_files:
            raise RuntimeError('no dist files found')
        for cmd, _, local_file in self.distribution.dist_files:
            if not 'bdist_wheel' == cmd:
                continue
            artifact = self._upload_library(local_file)
            self._install_library(artifact)
