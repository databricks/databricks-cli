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
from click import Context, Command
from click.testing import CliRunner
import requests

import databricks_cli.pipelines.cli as cli
from tests.utils import provide_conf

PIPELINE_SETTINGS_NO_ID = '{"name": "asdf"}'
PIPELINE_SETTINGS = '{"id": "123", "name": "asdf"}'
PIPELINE_ID = "123"


@pytest.fixture()
def pipelines_api_mock():
    with mock.patch('databricks_cli.pipelines.cli.PipelinesApi') as PipelinesApiMock:
        _pipelines_api_mock = mock.MagicMock()
        PipelinesApiMock.return_value = _pipelines_api_mock
        yield _pipelines_api_mock


@pytest.fixture()
def click_ctx():
    """
    A dummy Click context to allow testing of methods that raise exceptions. Fixes Click capturing
    actual exceptions and raising its own `RuntimeError: There is no active click context`.
    """
    return Context(Command('cmd'))


@provide_conf
def test_create_pipeline_settings_arg(pipelines_api_mock, tmpdir):
    pipelines_api_mock.create = mock.Mock(return_value={"pipeline_id": PIPELINE_ID})

    path = tmpdir.join('/settings.json').strpath
    with open(path, 'w') as f:
        f.write(PIPELINE_SETTINGS_NO_ID)

    runner = CliRunner()
    result = runner.invoke(cli.deploy_cli, [path])

    assert PIPELINE_ID in result.stdout


@provide_conf
def test_create_pipeline_settings_option(pipelines_api_mock, tmpdir):
    pipelines_api_mock.create = mock.Mock(return_value={"pipeline_id": PIPELINE_ID})

    path = tmpdir.join('/settings.json').strpath
    with open(path, 'w') as f:
        f.write(PIPELINE_SETTINGS_NO_ID)

    runner = CliRunner()
    result = runner.invoke(cli.deploy_cli, ['--spec', path])

    assert PIPELINE_ID in result.stdout


@provide_conf
def test_edit_and_deploy_cli_settings_arg(pipelines_api_mock, tmpdir):
    path = tmpdir.join('/settings.json').strpath
    with open(path, 'w') as f:
        f.write(PIPELINE_SETTINGS)
    runner = CliRunner()
    for cmd in [cli.deploy_cli, cli.edit_cli]:
        pipelines_api_mock.reset_mock()
        runner.invoke(cmd, [path])
        assert pipelines_api_mock.edit.call_args[0][0] == json.loads(PIPELINE_SETTINGS)


@provide_conf
def test_create_cli_settings_arg(pipelines_api_mock, tmpdir):
    path = tmpdir.join('/settings.json').strpath
    with open(path, 'w') as f:
        f.write(PIPELINE_SETTINGS_NO_ID)
    runner = CliRunner()
    for cmd in [cli.deploy_cli, cli.create_cli]:
        pipelines_api_mock.reset_mock()
        runner.invoke(cmd, [path])
        assert pipelines_api_mock.create.call_args[0][0] == json.loads(PIPELINE_SETTINGS_NO_ID)


@provide_conf
def test_deploy_settings_option(pipelines_api_mock, tmpdir):
    path = tmpdir.join('/settings.json').strpath
    with open(path, 'w') as f:
        f.write(PIPELINE_SETTINGS)

    path_no_id = tmpdir.join('/settings_no_id.json').strpath
    with open(path_no_id, 'w') as f:
        f.write(PIPELINE_SETTINGS_NO_ID)

    runner = CliRunner()
    for option in ['--settings', '--spec']:
        pipelines_api_mock.reset_mock()
        runner.invoke(cli.deploy_cli, [option, path])
        assert pipelines_api_mock.edit.call_args[0][0] == json.loads(PIPELINE_SETTINGS)

        runner.invoke(cli.deploy_cli, [option, path_no_id])
        assert pipelines_api_mock.create.call_args[0][0] == json.loads(PIPELINE_SETTINGS_NO_ID)


@provide_conf
def test_deploy_cli_incorrect_parameters(pipelines_api_mock, tmpdir):
    path = tmpdir.join('/settings.json').strpath
    with open(path, 'w') as f:
        f.write(PIPELINE_SETTINGS)
    runner = CliRunner()

    for option in ['--settings', '--spec']:
        pipelines_api_mock.reset_mock()
        result = runner.invoke(cli.deploy_cli, [path, option, path])
        assert result.exit_code == 1
        assert pipelines_api_mock.edit.call_count == 0
        result = runner.invoke(cli.deploy_cli, [option, path, path])
        assert result.exit_code == 1
        assert pipelines_api_mock.edit.call_count == 0


@provide_conf
def test_only_one_of_settings_or_settings_should_be_provided(pipelines_api_mock, tmpdir):
    path = tmpdir.join('/settings.json').strpath
    with open(path, 'w') as f:
        f.write(PIPELINE_SETTINGS)
    runner = CliRunner()
    result = runner.invoke(cli.deploy_cli, [path, '--settings', path, '--spec', path])
    assert "ValueError: Settings should be provided" in result.stdout
    assert pipelines_api_mock.create.call_count == 0
    assert pipelines_api_mock.edit.call_count == 0


@provide_conf
def test_delete_cli_id(pipelines_api_mock):
    runner = CliRunner()
    runner.invoke(cli.delete_cli, ['--pipeline-id', PIPELINE_ID])
    assert pipelines_api_mock.delete.call_args[0][0] == PIPELINE_ID


@provide_conf
def test_delete_cli_correct_parameters(pipelines_api_mock):
    runner = CliRunner()
    result = runner.invoke(cli.delete_cli, ['--pipeline-id', PIPELINE_ID])
    assert result.exit_code == 0
    assert pipelines_api_mock.delete.call_count == 1


@provide_conf
def test_deploy_settings_pipeline_id_is_not_changed_if_provided_in_spec(tmpdir):
    path = tmpdir.join('/settings.json').strpath
    with open(path, 'w') as f:
        f.write(PIPELINE_SETTINGS)
    runner = CliRunner()
    result = runner.invoke(cli.deploy_cli, ['--settings', path])
    assert '123' in result.stdout


@provide_conf
def test_correct_settings_extensions(pipelines_api_mock, tmpdir):
    pipelines_api_mock.create = mock.Mock(return_value={"pipeline_id": PIPELINE_ID})

    runner = CliRunner()

    path_no_extension = tmpdir.join('/settings').strpath
    with open(path_no_extension, 'w') as f:
        f.write(PIPELINE_SETTINGS_NO_ID)

    for cmd in [cli.deploy_cli, cli.create_cli]:
        pipelines_api_mock.reset_mock()
        result = runner.invoke(cmd, ['--settings', path_no_extension])
        assert result.exit_code == 0
        assert pipelines_api_mock.create.call_count == 1

    path_json = tmpdir.join('/settings.json').strpath
    with open(path_json, 'w') as f:
        f.write(PIPELINE_SETTINGS_NO_ID)

    for cmd in [cli.deploy_cli, cli.create_cli]:
        pipelines_api_mock.reset_mock()
        result = runner.invoke(cmd, ['--settings', path_json])
        assert result.exit_code == 0
        assert pipelines_api_mock.create.call_count == 1

    for cmd in [cli.deploy_cli, cli.edit_cli]:
        pipelines_api_mock.reset_mock()
        result = runner.invoke(cmd, ['--settings', path_json, '--pipeline-id', PIPELINE_ID])
        assert result.exit_code == 0
        assert pipelines_api_mock.edit.call_count == 1

    result = runner.invoke(cli.delete_cli, ['--pipeline-id', PIPELINE_ID])
    assert result.exit_code == 0
    assert pipelines_api_mock.delete.call_count == 1

    path_case_insensitive = tmpdir.join('/settings2.JsON').strpath
    with open(path_case_insensitive, 'w') as f:
        f.write(PIPELINE_SETTINGS_NO_ID)

    for cmd in [cli.deploy_cli, cli.create_cli]:
        pipelines_api_mock.reset_mock()
        result = runner.invoke(cmd, ['--settings', path_case_insensitive])
        assert result.exit_code == 0
        assert pipelines_api_mock.create.call_count == 1

    for cmd in [cli.deploy_cli, cli.edit_cli]:
        pipelines_api_mock.reset_mock()
        result = runner.invoke(cmd, [
            '--settings', path_case_insensitive,
            '--pipeline-id', PIPELINE_ID
        ])
        assert result.exit_code == 0
        assert pipelines_api_mock.edit.call_count == 1

    result = runner.invoke(cli.delete_cli, ['--pipeline-id', PIPELINE_ID])
    assert result.exit_code == 0
    assert pipelines_api_mock.delete.call_count == 1
    pipelines_api_mock.reset_mock()


@provide_conf
def test_invalid_settings_extension(pipelines_api_mock):
    for cmd in [cli.deploy_cli, cli.create_cli, cli.edit_cli]:
        pipelines_api_mock.reset_mock()
        result = CliRunner().invoke(cmd, ['--settings', 'settings.invalid'])
        assert result.exit_code == 1
        assert "ValueError: The provided file extension for the settings is not " \
               "supported" in result.stdout
        assert pipelines_api_mock.edit.call_count == 0
        assert pipelines_api_mock.create.call_count == 0


def test_gen_start_update_msg():
    assert "Started an update for pipeline" in \
           cli._gen_start_update_msg(None, "pipeline-id", False)
    assert "Started an update with full refresh for pipeline" in \
           cli._gen_start_update_msg(None, "pipeline-id", True)
    resp = {'update_id': "abc"}
    assert "Started an update abc with full refresh for pipeline" in \
           cli._gen_start_update_msg(resp, "pipeline-id", True)
    assert "Started an update abc for pipeline" in \
           cli._gen_start_update_msg(resp, "pipeline-id", False)


@provide_conf
def test_cli_id(pipelines_api_mock):
    runner = CliRunner()
    runner.invoke(cli.reset_cli, ['--pipeline-id', PIPELINE_ID])
    runner.invoke(cli.run_cli, ['--pipeline-id', PIPELINE_ID])
    runner.invoke(cli.start_cli, ['--pipeline-id', PIPELINE_ID])
    runner.invoke(cli.start_cli, ['--pipeline-id', PIPELINE_ID, "--full-refresh"])

    start_update_call_args_list = pipelines_api_mock.start_update.call_args_list
    assert start_update_call_args_list[0] == mock.call(PIPELINE_ID, full_refresh=True)
    assert start_update_call_args_list[1] == mock.call(PIPELINE_ID, full_refresh=False)
    assert start_update_call_args_list[2] == mock.call(PIPELINE_ID, full_refresh=False)
    assert start_update_call_args_list[3] == mock.call(PIPELINE_ID, full_refresh=True)


@provide_conf
def test_cli_no_id(pipelines_api_mock):
    for command in [cli.reset_cli, cli.stop_cli, cli.run_cli, cli.start_cli]:
        runner = CliRunner()
        result = runner.invoke(command, [])
        assert result.exit_code == 1
        assert pipelines_api_mock.reset.call_count == 0


@provide_conf
def test_get_cli_id(pipelines_api_mock):
    runner = CliRunner()
    runner.invoke(cli.get_cli, ['--pipeline-id', PIPELINE_ID])
    assert pipelines_api_mock.get.call_args[0][0] == PIPELINE_ID


@provide_conf
def test_get_cli_no_id(pipelines_api_mock):
    pipelines_api_mock.get.reset_mock()
    runner = CliRunner()
    result = runner.invoke(cli.get_cli, [])
    assert result.exit_code == 1
    assert pipelines_api_mock.get.call_count == 0


@provide_conf
def test_duplicate_name_check_error(pipelines_api_mock, tmpdir):
    mock_response = mock.MagicMock()
    mock_response.text = '{"error_code": "RESOURCE_CONFLICT"}'

    path = tmpdir.join('/settings.json').strpath
    with open(path, 'w') as f:
        f.write(PIPELINE_SETTINGS_NO_ID)
    runner = CliRunner()
    for cmd in [cli.deploy_cli, cli.create_cli]:
        pipelines_api_mock.reset_mock()
        pipelines_api_mock.create = mock.Mock(
            side_effect=requests.exceptions.HTTPError(response=mock_response))
        result = runner.invoke(cmd, [path])
        assert pipelines_api_mock.create.call_count == 1
        assert result.exit_code == 1
        assert "already exists" in result.stdout

    with open(path, 'w') as f:
        f.write(PIPELINE_SETTINGS)
    for cmd in [cli.deploy_cli, cli.edit_cli]:
        pipelines_api_mock.reset_mock()
        pipelines_api_mock.edit = mock.Mock(
            side_effect=requests.exceptions.HTTPError(response=mock_response))
        result = runner.invoke(cmd, [path])
        assert result.exit_code == 1
        assert pipelines_api_mock.edit.call_count == 1
        assert "already exists" in result.stdout


@provide_conf
def test_allow_duplicate_names_flag(pipelines_api_mock, tmpdir):
    path = tmpdir.join('/settings.json').strpath
    with open(path, 'w') as f:
        f.write(PIPELINE_SETTINGS_NO_ID)
    runner = CliRunner()

    for cmd in [cli.deploy_cli, cli.create_cli]:
        pipelines_api_mock.reset_mock()
        runner.invoke(cmd, [path])
        assert pipelines_api_mock.create.call_args_list[0][0][2] is False

        runner.invoke(cmd, [path, "--allow-duplicate-names"])
        assert pipelines_api_mock.create.call_args_list[1][0][2] is True

    with open(path, 'w') as f:
        f.write(PIPELINE_SETTINGS)

    for cmd in [cli.deploy_cli, cli.edit_cli]:
        pipelines_api_mock.reset_mock()
        runner.invoke(cmd, [path])
        assert pipelines_api_mock.edit.call_args_list[0][0][2] is False

        runner.invoke(cmd, [path, "--allow-duplicate-names"])
        assert pipelines_api_mock.edit.call_args_list[1][0][2] is True


@provide_conf
def test_create_pipeline_no_update_spec(pipelines_api_mock, tmpdir):
    path = tmpdir.join('/settings.json').strpath
    with open(path, 'w') as f:
        f.write(PIPELINE_SETTINGS_NO_ID)
    runner = CliRunner()

    for cmd in [cli.deploy_cli, cli.create_cli]:
        pipelines_api_mock.create = mock.Mock(return_value={"pipeline_id": PIPELINE_ID})
        result = runner.invoke(cmd, [path])

        assert result.exit_code == 0
        assert pipelines_api_mock.create.call_count == 1
        with open(path, 'r') as f:
            spec = json.loads(f.read())
        assert 'id' not in spec


@provide_conf
def test_create_with_id(pipelines_api_mock, tmpdir):
    path = tmpdir.join('/settings.json').strpath
    with open(path, 'w') as f:
        f.write(PIPELINE_SETTINGS)

    result = CliRunner().invoke(cli.create_cli, ['--settings', path])
    assert result.exit_code == 1
    assert "ValueError: Pipeline settings shouldn't contain \"id\"" in result.stdout
    assert pipelines_api_mock.create.call_count == 0


@provide_conf
def test_pipeline_conflicting_ids(pipelines_api_mock, tmpdir):
    path = tmpdir.join('/settings.json').strpath
    with open(path, 'w') as f:
        f.write(PIPELINE_SETTINGS)
    for cmd in [cli.deploy_cli, cli.edit_cli]:
        pipelines_api_mock.reset_mock()
        result = CliRunner().invoke(cmd, ['--settings', path, '--pipeline-id', "fake"])
        assert result.exit_code == 1
        assert "ValueError: The ID provided in --pipeline_id 'fake' is different from the ID " \
               "provided in the settings '123'." in result.stdout
        assert pipelines_api_mock.deploy.call_count == 0


@provide_conf
def test_with_missing_settings(pipelines_api_mock):
    for cmd in [cli.deploy_cli, cli.edit_cli, cli.create_cli]:
        pipelines_api_mock.reset_mock()
        result = CliRunner().invoke(cmd, [])
        assert result.exit_code == 1
        assert "ValueError: Settings should be provided" in result.stdout
        assert pipelines_api_mock.create.call_count == 0
        assert pipelines_api_mock.edit.call_count == 0
