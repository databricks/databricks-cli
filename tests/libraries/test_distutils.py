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

# pylint:disable=redefined-outer-name

import mock
import pytest
import setuptools.dist as dist

import databricks_cli.libraries.distutils as st
from databricks_cli.utils import InvalidConfigurationError
from databricks_cli.configure.provider import DatabricksConfig, \
    DatabricksConfigProvider, set_config_provider


@pytest.fixture()
def libraries_api_mock():
    with mock.patch('databricks_cli.libraries.cli.LibrariesApi') as LibrariesApi:
        _libraries_api_mock = mock.MagicMock()
        LibrariesApi.return_value = _libraries_api_mock
        yield _libraries_api_mock


def test_nothing():
    d = dist.Distribution()
    ilc = st.InstallLibraryCommand(d)
    ilc.initialize_options()

    assert ilc.cluster_id is None
    assert ilc.cluster_name is None
    assert ilc.cluster_tag is None
    assert ilc.dbfs_path is None


def test_sets_name():
    d = dist.Distribution({'name': 'foo'})

    ilc = st.InstallLibraryCommand(d)
    ilc.initialize_options()
    ilc.cluster_id = 'abc'
    ilc.finalize_options()

    assert ilc.dbfs_path == 'dbfs:/FileStore/jars/foo'


def test_gets_api_client():
    class TestConfigProvider(DatabricksConfigProvider):
        def get_config(self):
            return DatabricksConfig.from_token("Override", "Token!")
    provider = TestConfigProvider()
    set_config_provider(provider)

    d = dist.Distribution({'name': 'foo'})

    ilc = st.InstallLibraryCommand(d)
    ilc.initialize_options()
    ilc.cluster_id = 'abc'
    ilc.finalize_options()

    api_client = ilc._configure_api()
    assert api_client is not None


def test_gets_api_client_with_profile():
    import time
    d = dist.Distribution({'name': 'foo'})

    ilc = st.InstallLibraryCommand(d)
    ilc.initialize_options()
    ilc.cluster_id = 'abc'
    ilc.databricks_cli_profile = str(time.time())
    ilc.finalize_options()

    with pytest.raises(InvalidConfigurationError):
        ilc._configure_api()


def test_upload_library():
    class TestConfigProvider(DatabricksConfigProvider):
        def get_config(self):
            return DatabricksConfig.from_token("Override", "Token!")
    provider = TestConfigProvider()
    set_config_provider(provider)

    d = dist.Distribution({'name': 'foo'})

    ilc = st.InstallLibraryCommand(d)
    ilc.initialize_options()
    ilc.cluster_id = 'abc'
    ilc.finalize_options()

    with mock.patch('databricks_cli.dbfs.api.DbfsApi.put_file') as put_file:
        result = ilc._upload_library('foo/foo-0.0.1.wheel')
        assert result == 'dbfs:/FileStore/jars/foo/foo-0.0.1.wheel'
        put_file.assert_called_once()
        assert put_file.call_args[0][0] == 'foo/foo-0.0.1.wheel'


def test_run_stuff():
    d = dist.Distribution({'name': 'foo'})
    d.dist_files = [('bdist_wheel', None, 'foo/foo-0.0.1.wheel')]

    ilc = st.InstallLibraryCommand(d)
    ilc.initialize_options()
    ilc.cluster_id = 'abc'
    ilc.finalize_options()

    def _upload_library(f):
        assert f == 'foo/foo-0.0.1.wheel'
        return 'realpath'

    def _install_library(f):
        assert f == 'realpath'

    ilc._upload_library = _upload_library
    ilc._install_library = _install_library

    with mock.patch('distutils.cmd.Command.run_command') as run_command:
        ilc.run()
        run_command.assert_called_once()