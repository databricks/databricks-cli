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
import itertools
import mock
from click.testing import CliRunner

import databricks_cli.libraries.cli as cli
from databricks_cli.utils import pretty_format
from tests.utils import provide_conf, assert_cli_output

TEST_CLUSTER_ID = '0213-212348-veeps379'
ALL_CLUSTER_STATUSES_RETURN = {
    'statuses': [{
        'library_statuses': [{
            'status': 'INSTALLED',
            'is_library_for_all_clusters': False,
            'library': {
                'jar': 'dbfs:/test.jar'
            }
        }],
        'cluster_id': TEST_CLUSTER_ID
    }]
}


@provide_conf
def test_all_cluster_statuses_cli():
    with mock.patch('databricks_cli.libraries.cli.all_cluster_statuses') as \
            all_cluster_statuses_mock:
        all_cluster_statuses_mock.return_value = ALL_CLUSTER_STATUSES_RETURN
        runner = CliRunner()
        res = runner.invoke(cli.all_cluster_statuses_cli)
        all_cluster_statuses_mock.assert_called_once()
        assert_cli_output(res.output, pretty_format(ALL_CLUSTER_STATUSES_RETURN))


@provide_conf
def test_list_cli_without_cluster_id():
    with mock.patch('databricks_cli.libraries.cli.all_cluster_statuses') as \
            all_cluster_statuses_mock:
        all_cluster_statuses_mock.return_value = ALL_CLUSTER_STATUSES_RETURN
        runner = CliRunner()
        res = runner.invoke(cli.list_cli)
        all_cluster_statuses_mock.assert_called_once()
        assert_cli_output(res.output, pretty_format(ALL_CLUSTER_STATUSES_RETURN))


CLUSTER_STATUS_RETURN = {
    'library_statuses': [{
        'status': 'INSTALLED',
        'is_library_for_all_clusters': False,
        'library': {
            'jar': 'dbfs:/test.jar',
        }
    }],
    'cluster_id': '0213-212348-veeps379'
}


@provide_conf
def test_cluster_status_cli():
    with mock.patch('databricks_cli.libraries.cli.cluster_status') as cluster_status_mock:
        cluster_status_mock.return_value = CLUSTER_STATUS_RETURN
        runner = CliRunner()
        res = runner.invoke(cli.cluster_status_cli, ['--cluster-id', TEST_CLUSTER_ID])
        cluster_status_mock.assert_called_with(TEST_CLUSTER_ID)
        assert_cli_output(res.output, pretty_format(CLUSTER_STATUS_RETURN))


@provide_conf
def test_list_cli_with_cluster_id():
    with mock.patch('databricks_cli.libraries.cli.cluster_status') as cluster_status_mock:
        cluster_status_mock.return_value = CLUSTER_STATUS_RETURN
        runner = CliRunner()
        res = runner.invoke(cli.list_cli, ['--cluster-id', TEST_CLUSTER_ID])
        cluster_status_mock.assert_called_with(TEST_CLUSTER_ID)
        assert_cli_output(res.output, pretty_format(CLUSTER_STATUS_RETURN))


@provide_conf
def test_install_cli_with_multiple_oneof():
    for lib_a, lib_b in itertools.combinations(cli.INSTALL_OPTIONS, 2):
        with mock.patch('databricks_cli.libraries.cli.install_libraries') as install_libraries_mock:
            runner = CliRunner()
            res = runner.invoke(cli.install_cli, [
                '--cluster-id', TEST_CLUSTER_ID,
                '--{}'.format(lib_a), 'test_a',
                '--{}'.format(lib_b), 'test_b'])
            install_libraries_mock.assert_not_called()

            assert 'Only one of {} should be provided'.format(cli.INSTALL_OPTIONS) in res.output


@provide_conf
def test_install_cli_jar():
    test_jar = 'dbfs:/test.jar'
    with mock.patch('databricks_cli.libraries.cli.install_libraries') as install_libraries_mock:
        runner = CliRunner()
        runner.invoke(cli.install_cli, [
            '--cluster-id', TEST_CLUSTER_ID,
            '--jar', test_jar])
        install_libraries_mock.assert_called_with(TEST_CLUSTER_ID, [{'jar': test_jar}])


@provide_conf
def test_install_cli_egg():
    test_egg = 'dbfs:/test.egg'
    with mock.patch('databricks_cli.libraries.cli.install_libraries') as install_libraries_mock:
        runner = CliRunner()
        runner.invoke(cli.install_cli, [
            '--cluster-id', TEST_CLUSTER_ID,
            '--egg', test_egg])
        install_libraries_mock.assert_called_with(TEST_CLUSTER_ID, [{'egg': test_egg}])


@provide_conf
def test_install_cli_maven():
    test_maven_coordinates = 'org.jsoup:jsoup:1.7.2'
    test_maven_repo = 'https://maven.databricks.com'
    test_maven_exclusions = ['a', 'b']
    # Coordinates
    with mock.patch('databricks_cli.libraries.cli.install_libraries') as install_libraries_mock:
        runner = CliRunner()
        runner.invoke(cli.install_cli, [
            '--cluster-id', TEST_CLUSTER_ID,
            '--maven-coordinates', test_maven_coordinates])
        install_libraries_mock.assert_called_with(TEST_CLUSTER_ID, [{
            'maven': {
                'coordinates': test_maven_coordinates
            }
        }])
    # Coordinates, Repo
    with mock.patch('databricks_cli.libraries.cli.install_libraries') as install_libraries_mock:
        runner = CliRunner()
        runner.invoke(cli.install_cli, [
            '--cluster-id', TEST_CLUSTER_ID,
            '--maven-coordinates', test_maven_coordinates,
            '--maven-repo', test_maven_repo])
        install_libraries_mock.assert_called_with(TEST_CLUSTER_ID, [{
            'maven': {
                'coordinates': test_maven_coordinates,
                'repo': test_maven_repo
            }
        }])
    # Coordinates, Repo, Exclusions
    with mock.patch('databricks_cli.libraries.cli.install_libraries') as install_libraries_mock:
        runner = CliRunner()
        runner.invoke(cli.install_cli, [
            '--cluster-id', TEST_CLUSTER_ID,
            '--maven-coordinates', test_maven_coordinates,
            '--maven-repo', test_maven_repo,
            '--maven-exclusion', test_maven_exclusions[0],
            '--maven-exclusion', test_maven_exclusions[1]])
        install_libraries_mock.assert_called_with(TEST_CLUSTER_ID, [{
            'maven': {
                'coordinates': test_maven_coordinates,
                'repo': test_maven_repo,
                'exclusions': test_maven_exclusions
            }
        }])


@provide_conf
def test_install_cli_pypi():
    test_pypi_package = 'databricks-cli'
    test_pypi_repo = 'https://pypi.databricks.com'
    # Coordinates
    with mock.patch('databricks_cli.libraries.cli.install_libraries') as install_libraries_mock:
        runner = CliRunner()
        runner.invoke(cli.install_cli, [
            '--cluster-id', TEST_CLUSTER_ID,
            '--pypi-package', test_pypi_package,
            '--pypi-repo', test_pypi_repo])
        install_libraries_mock.assert_called_with(TEST_CLUSTER_ID, [{
            'pypi': {
                'package': test_pypi_package,
                'repo': test_pypi_repo
            }
        }])


@provide_conf
def test_install_cli_cran():
    test_cran_package = 'cran-package'
    test_cran_repo = 'https://cran.databricks.com'
    # Coordinates
    with mock.patch('databricks_cli.libraries.cli.install_libraries') as install_libraries_mock:
        runner = CliRunner()
        runner.invoke(cli.install_cli, [
            '--cluster-id', TEST_CLUSTER_ID,
            '--cran-package', test_cran_package,
            '--cran-repo', test_cran_repo])
        install_libraries_mock.assert_called_with(TEST_CLUSTER_ID, [{
            'cran': {
                'package': test_cran_package,
                'repo': test_cran_repo
            }
        }])


@provide_conf
def test_uninstall_cli_with_multiple_oneof():
    for lib_a, lib_b in itertools.combinations(cli.INSTALL_OPTIONS, 2):
        with mock.patch('databricks_cli.libraries.cli.uninstall_libraries') as \
                uninstall_libraries_mock:
            runner = CliRunner()
            res = runner.invoke(cli.uninstall_cli, [
                '--cluster-id', TEST_CLUSTER_ID,
                '--{}'.format(lib_a), 'test_a',
                '--{}'.format(lib_b), 'test_b'])
            uninstall_libraries_mock.assert_not_called()

            assert 'Only one of {} should be provided'.format(cli.INSTALL_OPTIONS) in res.output


@provide_conf
def test_uninstall_cli_jar():
    test_jar = 'dbfs:/test.jar'
    with mock.patch('databricks_cli.libraries.cli.uninstall_libraries') as uninstall_libraries_mock:
        runner = CliRunner()
        runner.invoke(cli.uninstall_cli, [
            '--cluster-id', TEST_CLUSTER_ID,
            '--jar', test_jar])
        uninstall_libraries_mock.assert_called_with(TEST_CLUSTER_ID, [{'jar': test_jar}])


@provide_conf
def test_uninstall_cli_egg():
    test_egg = 'dbfs:/test.egg'
    with mock.patch('databricks_cli.libraries.cli.uninstall_libraries') as uninstall_libraries_mock:
        runner = CliRunner()
        runner.invoke(cli.uninstall_cli, [
            '--cluster-id', TEST_CLUSTER_ID,
            '--egg', test_egg])
        uninstall_libraries_mock.assert_called_with(TEST_CLUSTER_ID, [{'egg': test_egg}])


@provide_conf
def test_uninstall_cli_maven():
    test_maven_coordinates = 'org.jsoup:jsoup:1.7.2'
    test_maven_repo = 'https://maven.databricks.com'
    test_maven_exclusions = ['a', 'b']
    # Coordinates
    with mock.patch('databricks_cli.libraries.cli.uninstall_libraries') as uninstall_libraries_mock:
        runner = CliRunner()
        runner.invoke(cli.uninstall_cli, [
            '--cluster-id', TEST_CLUSTER_ID,
            '--maven-coordinates', test_maven_coordinates])
        uninstall_libraries_mock.assert_called_with(TEST_CLUSTER_ID, [{
            'maven': {
                'coordinates': test_maven_coordinates
            }
        }])
    # Coordinates, Repo
    with mock.patch('databricks_cli.libraries.cli.uninstall_libraries') as uninstall_libraries_mock:
        runner = CliRunner()
        runner.invoke(cli.uninstall_cli, [
            '--cluster-id', TEST_CLUSTER_ID,
            '--maven-coordinates', test_maven_coordinates,
            '--maven-repo', test_maven_repo])
        uninstall_libraries_mock.assert_called_with(TEST_CLUSTER_ID, [{
            'maven': {
                'coordinates': test_maven_coordinates,
                'repo': test_maven_repo
            }
        }])
    # Coordinates, Repo, Exclusions
    with mock.patch('databricks_cli.libraries.cli.uninstall_libraries') as uninstall_libraries_mock:
        runner = CliRunner()
        runner.invoke(cli.uninstall_cli, [
            '--cluster-id', TEST_CLUSTER_ID,
            '--maven-coordinates', test_maven_coordinates,
            '--maven-repo', test_maven_repo,
            '--maven-exclusion', test_maven_exclusions[0],
            '--maven-exclusion', test_maven_exclusions[1]])
        uninstall_libraries_mock.assert_called_with(TEST_CLUSTER_ID, [{
            'maven': {
                'coordinates': test_maven_coordinates,
                'repo': test_maven_repo,
                'exclusions': test_maven_exclusions
            }
        }])


@provide_conf
def test_uninstall_cli_pypi():
    test_pypi_package = 'databricks-cli'
    test_pypi_repo = 'https://pypi.databricks.com'
    # Coordinates
    with mock.patch('databricks_cli.libraries.cli.uninstall_libraries') as uninstall_libraries_mock:
        runner = CliRunner()
        runner.invoke(cli.uninstall_cli, [
            '--cluster-id', TEST_CLUSTER_ID,
            '--pypi-package', test_pypi_package,
            '--pypi-repo', test_pypi_repo])
        uninstall_libraries_mock.assert_called_with(TEST_CLUSTER_ID, [{
            'pypi': {
                'package': test_pypi_package,
                'repo': test_pypi_repo
            }
        }])


@provide_conf
def test_uninstall_cli_cran():
    test_cran_package = 'cran-package'
    test_cran_repo = 'https://cran.databricks.com'
    # Coordinates
    with mock.patch('databricks_cli.libraries.cli.uninstall_libraries') as uninstall_libraries_mock:
        runner = CliRunner()
        runner.invoke(cli.uninstall_cli, [
            '--cluster-id', TEST_CLUSTER_ID,
            '--cran-package', test_cran_package,
            '--cran-repo', test_cran_repo])
        uninstall_libraries_mock.assert_called_with(TEST_CLUSTER_ID, [{
            'cran': {
                'package': test_cran_package,
                'repo': test_cran_repo
            }
        }])
