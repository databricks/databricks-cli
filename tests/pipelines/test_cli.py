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

import json
import mock
import pytest
from click.testing import CliRunner

import databricks_cli.pipelines.cli as cli
from tests.utils import provide_conf

DEPLOY_SPEC = '{"id": "123"}'
PIPELINE_ID = "123"


@pytest.fixture()
def pipelines_api_mock():
    with mock.patch('databricks_cli.pipelines.cli.PipelinesApi') as PipelinesApiMock:
        _pipelines_api_mock = mock.MagicMock()
        PipelinesApiMock.return_value = _pipelines_api_mock
        yield _pipelines_api_mock


@provide_conf
def test_deploy_cli_spec_arg(pipelines_api_mock, tmpdir):
    path = tmpdir.join('/spec.json').strpath
    with open(path, 'w') as f:
        f.write(DEPLOY_SPEC)
    runner = CliRunner()
    runner.invoke(cli.deploy_cli, [path])
    assert pipelines_api_mock.deploy.call_args[0][0] == json.loads(DEPLOY_SPEC)


@provide_conf
def test_deploy_spec_option(pipelines_api_mock, tmpdir):
    path = tmpdir.join('/spec.json').strpath
    with open(path, 'w') as f:
        f.write(DEPLOY_SPEC)
    runner = CliRunner()
    runner.invoke(cli.deploy_cli, ['--spec', path])
    assert pipelines_api_mock.deploy.call_args[0][0] == json.loads(DEPLOY_SPEC)


@provide_conf
def test_deploy_cli_incorrect_parameters(pipelines_api_mock, tmpdir):
    path = tmpdir.join('/spec.json').strpath
    with open(path, 'w') as f:
        f.write(DEPLOY_SPEC)
    runner = CliRunner()
    result = runner.invoke(cli.deploy_cli, [path, '--spec', path])
    assert result.exit_code == 1
    assert pipelines_api_mock.deploy.call_count == 0
    result = runner.invoke(cli.deploy_cli, ['--spec', path, path])
    assert result.exit_code == 1
    assert pipelines_api_mock.deploy.call_count == 0


@provide_conf
def test_delete_cli_spec_arg(pipelines_api_mock, tmpdir):
    path = tmpdir.join('/spec.json').strpath
    with open(path, 'w') as f:
        f.write(DEPLOY_SPEC)
    runner = CliRunner()
    runner.invoke(cli.delete_cli, [path])
    assert pipelines_api_mock.delete.call_args[0][0] == PIPELINE_ID


@provide_conf
def test_delete_cli_spec_option(pipelines_api_mock, tmpdir):
    path = tmpdir.join('/spec.json').strpath
    with open(path, 'w') as f:
        f.write(DEPLOY_SPEC)
    runner = CliRunner()
    runner.invoke(cli.delete_cli, ['--spec', path])
    assert pipelines_api_mock.delete.call_args[0][0] == PIPELINE_ID


@provide_conf
def test_delete_cli_id(pipelines_api_mock):
    runner = CliRunner()
    runner.invoke(cli.delete_cli, ['--pipeline-id', PIPELINE_ID])
    assert pipelines_api_mock.delete.call_args[0][0] == PIPELINE_ID


@provide_conf
def test_delete_cli_incorrect_parameters(pipelines_api_mock, tmpdir):
    path = tmpdir.join('/spec.json').strpath
    with open(path, 'w') as f:
        f.write(DEPLOY_SPEC)
    runner = CliRunner()
    result = runner.invoke(cli.delete_cli, ['--spec', path, '--pipeline-id', PIPELINE_ID])
    assert result.exit_code == 1
    assert pipelines_api_mock.delete.call_count == 0
    result = runner.invoke(cli.delete_cli, ['--spec', path, path])
    assert result.exit_code == 1
    assert pipelines_api_mock.delete.call_count == 0
    result = runner.invoke(cli.delete_cli, [path, '--pipeline-id', PIPELINE_ID])
    assert result.exit_code == 1
    assert pipelines_api_mock.delete.call_count == 0
    result = runner.invoke(cli.delete_cli, [path, '--spec', path, '--pipeline-id', PIPELINE_ID])
    assert result.exit_code == 1
    assert pipelines_api_mock.delete.call_count == 0


@provide_conf
def test_deploy_delete_cli_incorrect_spec_extension(pipelines_api_mock, tmpdir):
    path = tmpdir.join('/spec.wrong_ext').strpath
    with open(path, 'w') as f:
        f.write(DEPLOY_SPEC)
    runner = CliRunner()
    result = runner.invoke(cli.deploy_cli, ['--spec', path])
    assert result.exit_code == 1
    assert pipelines_api_mock.deploy.call_count == 0

    result = runner.invoke(cli.delete_cli, ['--spec', path])
    assert result.exit_code == 1
    assert pipelines_api_mock.delete.call_count == 0

    path_no_extension = tmpdir.join('/spec').strpath
    with open(path_no_extension, 'w') as f:
        f.write(DEPLOY_SPEC)
    result = runner.invoke(cli.deploy_cli, ['--spec', path_no_extension])
    assert result.exit_code == 1
    assert pipelines_api_mock.deploy.call_count == 0

    result = runner.invoke(cli.delete_cli, ['--spec', path_no_extension])
    assert result.exit_code == 1
    assert pipelines_api_mock.delete.call_count == 0


@provide_conf
def test_deploy_delete_cli_correct_spec_extensions(pipelines_api_mock, tmpdir):
    runner = CliRunner()
    path_json = tmpdir.join('/spec.json').strpath
    with open(path_json, 'w') as f:
        f.write(DEPLOY_SPEC)
    result = runner.invoke(cli.deploy_cli, ['--spec', path_json])
    assert result.exit_code == 0
    assert pipelines_api_mock.deploy.call_count == 1
    result = runner.invoke(cli.delete_cli, ['--spec', path_json])
    assert result.exit_code == 0
    assert pipelines_api_mock.delete.call_count == 1
    pipelines_api_mock.reset_mock()

    path_case_insensitive = tmpdir.join('/spec2.JsON').strpath
    with open(path_case_insensitive, 'w') as f:
        f.write(DEPLOY_SPEC)
    result = runner.invoke(cli.deploy_cli, ['--spec', path_case_insensitive])
    assert result.exit_code == 0
    assert pipelines_api_mock.deploy.call_count == 1
    result = runner.invoke(cli.delete_cli, ['--spec', path_case_insensitive])
    assert result.exit_code == 0
    assert pipelines_api_mock.delete.call_count == 1
    pipelines_api_mock.reset_mock()


@provide_conf
def test_reset_cli_spec_arg(pipelines_api_mock, tmpdir):
    path = tmpdir.join('/spec.json').strpath
    with open(path, 'w') as f:
        f.write(DEPLOY_SPEC)
    runner = CliRunner()
    runner.invoke(cli.reset_cli, [path])
    assert pipelines_api_mock.reset.call_args[0][0] == PIPELINE_ID


@provide_conf
def test_reset_cli_spec_option(pipelines_api_mock, tmpdir):
    path = tmpdir.join('/spec.json').strpath
    with open(path, 'w') as f:
        f.write(DEPLOY_SPEC)
    runner = CliRunner()
    runner.invoke(cli.reset_cli, ['--spec', path])
    assert pipelines_api_mock.reset.call_args[0][0] == PIPELINE_ID


@provide_conf
def test_reset_cli_id(pipelines_api_mock):
    runner = CliRunner()
    runner.invoke(cli.reset_cli, ['--pipeline-id', PIPELINE_ID])
    assert pipelines_api_mock.reset.call_args[0][0] == PIPELINE_ID


@provide_conf
def test_reset_cli_no_id(pipelines_api_mock):
    runner = CliRunner()
    result = runner.invoke(cli.reset_cli, [])
    assert result.exit_code == 1
    assert pipelines_api_mock.reset.call_count == 0


@provide_conf
def test_get_cli_spec_arg(pipelines_api_mock, tmpdir):
    path = tmpdir.join('/spec.json').strpath
    with open(path, 'w') as f:
        f.write(DEPLOY_SPEC)
    runner = CliRunner()
    runner.invoke(cli.get_cli, [path])
    assert pipelines_api_mock.get.call_args[0][0] == PIPELINE_ID


@provide_conf
def test_get_cli_spec_option(pipelines_api_mock, tmpdir):
    path = tmpdir.join('/spec.json').strpath
    with open(path, 'w') as f:
        f.write(DEPLOY_SPEC)
    runner = CliRunner()
    runner.invoke(cli.get_cli, ['--spec', path])
    assert pipelines_api_mock.get.call_args[0][0] == PIPELINE_ID


@provide_conf
def test_get_cli_id(pipelines_api_mock):
    runner = CliRunner()
    runner.invoke(cli.get_cli, ['--pipeline-id', PIPELINE_ID])
    assert pipelines_api_mock.get.call_args[0][0] == PIPELINE_ID


@provide_conf
def test_get_cli_no_id(pipelines_api_mock):
    runner = CliRunner()
    result = runner.invoke(cli.get_cli, [])
    assert result.exit_code == 1
    assert pipelines_api_mock.get.call_count == 0
