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

# pylint:disable=redefined-outer-name
import itertools
import mock
import pytest
from click.testing import CliRunner

import databricks_cli.libraries.cli as cli
from databricks_cli.utils import pretty_format
from tests.test_data import TEST_CLUSTER_NAME, ALL_CLUSTER_STATUSES_RETURN, \
    TEST_CLUSTER_ID 
from tests.utils import provide_conf, assert_cli_output


@pytest.fixture()
def cluster_api_mock():
    with mock.patch('databricks_cli.libraries.cli.ClusterApi') as ClusterApiMock:
        _cluster_api_mock = mock.MagicMock()
        ClusterApiMock.return_value = _cluster_api_mock
        # make sure we always get a cluster name back
        rv = {'cluster_name': TEST_CLUSTER_NAME}
        _cluster_api_mock.get_cluster = mock.MagicMock(return_value=rv)

        yield _cluster_api_mock


@pytest.fixture()
def cluster_sdk_mock():
    with mock.patch('databricks_cli.clusters.api.ClusterService') as ClusterSdkMock:
        _cluster_sdk_mock = mock.MagicMock()
        ClusterSdkMock.return_value = _cluster_sdk_mock
        rv = {'cluster_name': TEST_CLUSTER_NAME}
        _cluster_sdk_mock.get_cluster = mock.MagicMock(return_value=rv)

        yield _cluster_sdk_mock


@pytest.fixture()
def libraries_sdk_mock():
    with mock.patch('databricks_cli.libraries.api.ManagedLibraryService') as ManagedLibraryService:
        _managed_library_service_mock = mock.MagicMock()
        ManagedLibraryService.return_value = _managed_library_service_mock

        yield _managed_library_service_mock


@provide_conf
def test_all_cluster_statuses_cli(libraries_sdk_mock):
    libraries_sdk_mock.all_cluster_statuses.return_value = ALL_CLUSTER_STATUSES_RETURN
    runner = CliRunner()
    res = runner.invoke(cli.all_cluster_statuses_cli)
    libraries_sdk_mock.all_cluster_statuses.assert_called_once()
    assert_cli_output(res.output, pretty_format(ALL_CLUSTER_STATUSES_RETURN))


@provide_conf
def test_list_cli_without_cluster_id(libraries_sdk_mock):
    libraries_sdk_mock.all_cluster_statuses.return_value = ALL_CLUSTER_STATUSES_RETURN
    runner = CliRunner()
    res = runner.invoke(cli.list_cli)
    libraries_sdk_mock.all_cluster_statuses.assert_called_once()
    assert_cli_output(res.output, pretty_format(ALL_CLUSTER_STATUSES_RETURN))


CLUSTER_STATUS_RETURN = {
    'library_statuses': [{
        'status': 'INSTALLED',
        'is_library_for_all_clusters': False,
        'library': {
            'jar': 'dbfs:/test.jar',
        }
    }],
    'cluster_id': TEST_CLUSTER_ID
}


@provide_conf
def test_cluster_status_cli(libraries_sdk_mock):
    libraries_sdk_mock.cluster_status.return_value = CLUSTER_STATUS_RETURN
    runner = CliRunner()
    res = runner.invoke(cli.cluster_status_cli, ['--cluster-id', TEST_CLUSTER_ID])
    libraries_sdk_mock.cluster_status.assert_called_with(TEST_CLUSTER_ID)
    assert_cli_output(res.output, pretty_format(CLUSTER_STATUS_RETURN))


@provide_conf
def test_list_cli_with_cluster_id(libraries_sdk_mock):
    libraries_sdk_mock.cluster_status.return_value = CLUSTER_STATUS_RETURN
    runner = CliRunner()
    res = runner.invoke(cli.list_cli, ['--cluster-id', TEST_CLUSTER_ID])
    libraries_sdk_mock.cluster_status.assert_called_with(TEST_CLUSTER_ID)
    assert_cli_output(res.output, pretty_format(CLUSTER_STATUS_RETURN))


@provide_conf
def test_install_cli_with_multiple_oneof(libraries_sdk_mock):
    for lib_a, lib_b in itertools.combinations(cli.INSTALL_OPTIONS, 2):
        runner = CliRunner()
        res = runner.invoke(cli.install_cli, [
            '--cluster-id', TEST_CLUSTER_ID,
            '--{}'.format(lib_a), 'test_a',
            '--{}'.format(lib_b), 'test_b'])
        libraries_sdk_mock.install_libraries.assert_not_called()

        assert 'Only one of {} should be provided'.format(cli.INSTALL_OPTIONS) in res.output


@provide_conf
def test_install_cli_jar(libraries_sdk_mock):
    test_jar = 'dbfs:/test.jar'
    runner = CliRunner()
    runner.invoke(cli.install_cli, [
        '--cluster-id', TEST_CLUSTER_ID,
        '--jar', test_jar])
    libraries_sdk_mock.install_libraries.assert_called_with(TEST_CLUSTER_ID, [{'jar': test_jar}])


@provide_conf
def test_install_cli_egg(libraries_sdk_mock):
    test_egg = 'dbfs:/test.egg'
    runner = CliRunner()
    runner.invoke(cli.install_cli, [
        '--cluster-id', TEST_CLUSTER_ID,
        '--egg', test_egg])
    libraries_sdk_mock.install_libraries.assert_called_with(TEST_CLUSTER_ID, [{'egg': test_egg}])


@provide_conf
def test_install_cli_wheel(libraries_sdk_mock):
    test_wheel = 'dbfs:/test.whl'
    runner = CliRunner()
    runner.invoke(cli.install_cli, [
        '--cluster-id', TEST_CLUSTER_ID,
        '--whl', test_wheel])
    libraries_sdk_mock.install_libraries.assert_called_with(TEST_CLUSTER_ID, [{'whl': test_wheel}])


@provide_conf
def test_install_cli_maven(libraries_sdk_mock):
    test_maven_coordinates = 'org.jsoup:jsoup:1.7.2'
    test_maven_repo = 'https://maven.databricks.com'
    test_maven_exclusions = ['a', 'b']
    # Coordinates
    runner = CliRunner()
    runner.invoke(cli.install_cli, [
        '--cluster-id', TEST_CLUSTER_ID,
        '--maven-coordinates', test_maven_coordinates])
    libraries_sdk_mock.install_libraries.assert_called_with(TEST_CLUSTER_ID, [{
        'maven': {
            'coordinates': test_maven_coordinates
        }
    }])
    # Coordinates, Repo
    runner = CliRunner()
    runner.invoke(cli.install_cli, [
        '--cluster-id', TEST_CLUSTER_ID,
        '--maven-coordinates', test_maven_coordinates,
        '--maven-repo', test_maven_repo])
    libraries_sdk_mock.install_libraries.assert_called_with(TEST_CLUSTER_ID, [{
        'maven': {
            'coordinates': test_maven_coordinates,
            'repo': test_maven_repo
        }
    }])
    # Coordinates, Repo, Exclusions
    runner = CliRunner()
    runner.invoke(cli.install_cli, [
        '--cluster-id', TEST_CLUSTER_ID,
        '--maven-coordinates', test_maven_coordinates,
        '--maven-repo', test_maven_repo,
        '--maven-exclusion', test_maven_exclusions[0],
        '--maven-exclusion', test_maven_exclusions[1]])
    libraries_sdk_mock.install_libraries.assert_called_with(TEST_CLUSTER_ID, [{
        'maven': {
            'coordinates': test_maven_coordinates,
            'repo': test_maven_repo,
            'exclusions': test_maven_exclusions
        }
    }])


@provide_conf
def test_install_cli_pypi(libraries_sdk_mock):
    test_pypi_package = 'databricks-cli'
    test_pypi_repo = 'https://pypi.databricks.com'
    # Coordinates
    runner = CliRunner()
    runner.invoke(cli.install_cli, [
        '--cluster-id', TEST_CLUSTER_ID,
        '--pypi-package', test_pypi_package,
        '--pypi-repo', test_pypi_repo])
    libraries_sdk_mock.install_libraries.assert_called_with(TEST_CLUSTER_ID, [{
        'pypi': {
            'package': test_pypi_package,
            'repo': test_pypi_repo
        }
    }])


@provide_conf
def test_install_cli_cran(libraries_sdk_mock):
    test_cran_package = 'cran-package'
    test_cran_repo = 'https://cran.databricks.com'
    # Coordinates
    runner = CliRunner()
    runner.invoke(cli.install_cli, [
        '--cluster-id', TEST_CLUSTER_ID,
        '--cran-package', test_cran_package,
        '--cran-repo', test_cran_repo])
    libraries_sdk_mock.install_libraries.assert_called_with(TEST_CLUSTER_ID, [{
        'cran': {
            'package': test_cran_package,
            'repo': test_cran_repo
        }
    }])


@provide_conf
def test_uninstall_cli_with_multiple_oneof(libraries_sdk_mock):
    for lib_a, lib_b in itertools.combinations(cli.INSTALL_OPTIONS, 2):
        runner = CliRunner()
        res = runner.invoke(cli.uninstall_cli, [
            '--cluster-id', TEST_CLUSTER_ID,
            '--{}'.format(lib_a), 'test_a',
            '--{}'.format(lib_b), 'test_b'])
        libraries_sdk_mock.uninstall_libraries.assert_not_called()

        assert 'Only one of {} should be provided'.format(cli.UNINSTALL_OPTIONS) in res.output


@provide_conf
def test_uninstall_cli_all(libraries_sdk_mock):
    test_jar = 'dbfs:/test.jar'
    runner = CliRunner()
    libraries_sdk_mock.cluster_status.return_value = {
        "library_statuses": [
            {
                "status": "INSTALLED",
                "is_library_for_all_clusters": False,
                "library": {
                    "jar": test_jar
                }
            }
        ],
        "cluster_id": TEST_CLUSTER_ID,
    }
    runner.invoke(cli.uninstall_cli, [
        '--cluster-id', TEST_CLUSTER_ID,
        '--all'])
    libraries_sdk_mock.uninstall_libraries.assert_called_with(TEST_CLUSTER_ID, [{'jar': test_jar}])


@provide_conf
def test_uninstall_cli_all_for_no_libraries(libraries_sdk_mock):
    runner = CliRunner()
    libraries_sdk_mock.cluster_status.return_value = {
        "library_statuses": [
        ],
        "cluster_id": TEST_CLUSTER_ID,
    }
    runner.invoke(cli.uninstall_cli, [
        '--cluster-id', TEST_CLUSTER_ID,
        '--all'])
    libraries_sdk_mock.uninstall_libraries.assert_not_called()


@provide_conf
def test_uninstall_cli_jar(libraries_sdk_mock):
    test_jar = 'dbfs:/test.jar'
    runner = CliRunner()
    runner.invoke(cli.uninstall_cli, [
        '--cluster-id', TEST_CLUSTER_ID,
        '--jar', test_jar])
    libraries_sdk_mock.uninstall_libraries.assert_called_with(TEST_CLUSTER_ID, [{'jar': test_jar}])


@provide_conf
def test_uninstall_cli_egg(libraries_sdk_mock):
    test_egg = 'dbfs:/test.egg'
    runner = CliRunner()
    runner.invoke(cli.uninstall_cli, [
        '--cluster-id', TEST_CLUSTER_ID,
        '--egg', test_egg])
    libraries_sdk_mock.uninstall_libraries.assert_called_with(TEST_CLUSTER_ID, [{'egg': test_egg}])


@provide_conf
def test_uninstall_cli_whl(libraries_sdk_mock):
    test_whl = 'dbfs:/test.whl'
    runner = CliRunner()
    runner.invoke(cli.uninstall_cli, [
        '--cluster-id', TEST_CLUSTER_ID,
        '--whl', test_whl])
    libraries_sdk_mock.uninstall_libraries.assert_called_with(TEST_CLUSTER_ID, [{'whl': test_whl}])


@provide_conf
def test_uninstall_cli_maven(libraries_sdk_mock):
    test_maven_coordinates = 'org.jsoup:jsoup:1.7.2'
    test_maven_repo = 'https://maven.databricks.com'
    test_maven_exclusions = ['a', 'b']
    # Coordinates
    runner = CliRunner()
    runner.invoke(cli.uninstall_cli, [
        '--cluster-id', TEST_CLUSTER_ID,
        '--maven-coordinates', test_maven_coordinates])
    libraries_sdk_mock.uninstall_libraries.assert_called_with(TEST_CLUSTER_ID, [{
        'maven': {
            'coordinates': test_maven_coordinates
        }
    }])
    # Coordinates, Repo
    runner = CliRunner()
    runner.invoke(cli.uninstall_cli, [
        '--cluster-id', TEST_CLUSTER_ID,
        '--maven-coordinates', test_maven_coordinates,
        '--maven-repo', test_maven_repo])
    libraries_sdk_mock.uninstall_libraries.assert_called_with(TEST_CLUSTER_ID, [{
        'maven': {
            'coordinates': test_maven_coordinates,
            'repo': test_maven_repo
        }
    }])
    # Coordinates, Repo, Exclusions
    runner = CliRunner()
    runner.invoke(cli.uninstall_cli, [
        '--cluster-id', TEST_CLUSTER_ID,
        '--maven-coordinates', test_maven_coordinates,
        '--maven-repo', test_maven_repo,
        '--maven-exclusion', test_maven_exclusions[0],
        '--maven-exclusion', test_maven_exclusions[1]])
    libraries_sdk_mock.uninstall_libraries.assert_called_with(TEST_CLUSTER_ID, [{
        'maven': {
            'coordinates': test_maven_coordinates,
            'repo': test_maven_repo,
            'exclusions': test_maven_exclusions
        }
    }])


@provide_conf
def test_uninstall_cli_pypi(libraries_sdk_mock):
    test_pypi_package = 'databricks-cli'
    test_pypi_repo = 'https://pypi.databricks.com'
    # Coordinates
    runner = CliRunner()
    runner.invoke(cli.uninstall_cli, [
        '--cluster-id', TEST_CLUSTER_ID,
        '--pypi-package', test_pypi_package,
        '--pypi-repo', test_pypi_repo])
    libraries_sdk_mock.uninstall_libraries.assert_called_with(TEST_CLUSTER_ID, [{
        'pypi': {
            'package': test_pypi_package,
            'repo': test_pypi_repo
        }
    }])


@provide_conf
def test_uninstall_cli_cran(libraries_sdk_mock):
    test_cran_package = 'cran-package'
    test_cran_repo = 'https://cran.databricks.com'
    # Coordinates
    runner = CliRunner()
    runner.invoke(cli.uninstall_cli, [
        '--cluster-id', TEST_CLUSTER_ID,
        '--cran-package', test_cran_package,
        '--cran-repo', test_cran_repo])
    libraries_sdk_mock.uninstall_libraries.assert_called_with(TEST_CLUSTER_ID, [{
        'cran': {
            'package': test_cran_package,
            'repo': test_cran_repo
        }
    }])


@provide_conf
def test_list_cli_with_cluster_name(libraries_sdk_mock, cluster_api_mock):
    libraries_sdk_mock.cluster_status.return_value = CLUSTER_STATUS_RETURN
    cluster_api_mock.get_cluster_id_for_name.return_value = TEST_CLUSTER_ID
    runner = CliRunner()
    runner.invoke(cli.list_cli, ['--cluster-name', TEST_CLUSTER_NAME])
    libraries_sdk_mock.cluster_status.assert_called_with(TEST_CLUSTER_ID)
