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
# TODO(alinxie)Write test which deploys a stack and validates the status json.
# pylint:disable=redefined-outer-name
# pylint:disable=unused-argument

import os
import json
import pytest
import mock
from click.testing import CliRunner

import databricks_cli.stack.cli as cli
from tests.utils import provide_conf
from tests.stack.test_api import TEST_STATUS, TEST_STACK

TEST_STACK_PATH = 'stack/stack.json'


def _write_test_stack_config(tmpdir):
    """
    Utility function to store the stack config in the filesystem and returns the config path.
    """
    config_working_dir = os.path.join(tmpdir.strpath, 'stack')
    config_path = os.path.join(config_working_dir, 'test.json')
    os.makedirs(config_working_dir)
    with open(config_path, 'w+') as f:
        json.dump(TEST_STACK, f)
    return config_path


@pytest.fixture()
def stack_api_mock():
    with mock.patch('databricks_cli.stack.cli.StackApi') as StackApiMock:
        stack_api_mock = mock.MagicMock()
        StackApiMock.return_value = stack_api_mock
        yield stack_api_mock


@provide_conf
def test_deploy_kwargs(stack_api_mock, tmpdir):
    """
    Calling the cli.deploy command should call the deploy function of the stack API and
    pass in possible kwargs into deploy function
    """
    config_path = _write_test_stack_config(tmpdir)
    stack_api_mock.deploy = mock.MagicMock()
    runner = CliRunner()
    runner.invoke(cli.deploy, ['--overwrite', config_path])
    stack_api_mock.deploy.assert_called()
    assert stack_api_mock.deploy.call_args[0][0] == TEST_STACK
    # Check overwrite in kwargs
    assert stack_api_mock.deploy.call_args[1]['overwrite'] is True


@provide_conf
def test_download_kwargs(stack_api_mock, tmpdir):
    """
    Calling the cli.deploy command should call the deploy function of the stack API and
    pass in possible kwargs into deploy function
    """
    config_path = _write_test_stack_config(tmpdir)
    stack_api_mock.download = mock.MagicMock()
    runner = CliRunner()
    runner.invoke(cli.download, ['--overwrite', config_path])
    stack_api_mock.download.assert_called()
    assert stack_api_mock.download.call_args[0][0] == TEST_STACK
    # Check overwrite in kwargs
    assert stack_api_mock.download.call_args[1]['overwrite'] is True


@provide_conf
def test_deploy_default_kwargs(stack_api_mock, tmpdir):
    """
    Calling the cli.deploy command without flags should call the deploy function of the stack API
    and pass in default kwargs into deploy function
    """
    config_path = _write_test_stack_config(tmpdir)
    stack_api_mock.deploy = mock.MagicMock()
    runner = CliRunner()
    runner.invoke(cli.deploy, [config_path])
    stack_api_mock.deploy.assert_called()
    assert stack_api_mock.deploy.call_args[0][0] == TEST_STACK
    # Check overwrite in kwargs
    assert stack_api_mock.deploy.call_args[1]['overwrite'] is False


@provide_conf
def test_download_default_kwargs(stack_api_mock, tmpdir):
    """
    Calling the cli.deploy command should call the deploy function of the stack API and
    pass in possible kwargs into deploy function
    """
    config_path = _write_test_stack_config(tmpdir)
    stack_api_mock.download = mock.MagicMock()
    runner = CliRunner()
    runner.invoke(cli.download, [config_path])
    stack_api_mock.download.assert_called()
    assert stack_api_mock.download.call_args[0][0] == TEST_STACK
    # Check overwrite in kwargs
    assert stack_api_mock.download.call_args[1]['overwrite'] is False


@provide_conf
def test_deploy_relative_paths(stack_api_mock, tmpdir):
    """
        When cli.deploy calls on StackApi.deploy, the current
        working directory should be the same directory as where the stack config template is
        contained so that relative paths for resources can be relative to the stack config
        instead of where CLI calls the API functions.
    """
    config_path = _write_test_stack_config(tmpdir)
    config_working_dir = os.path.dirname(config_path)

    def _deploy(*args, **kwargs):
        assert os.getcwd() == config_working_dir

    stack_api_mock.deploy = mock.Mock(wraps=_deploy)
    runner = CliRunner()
    runner.invoke(cli.deploy, [config_path])


@provide_conf
def test_download_relative_paths(stack_api_mock, tmpdir):
    """
        When cli.download calls on StackApi.download, the current working
        working directory should be the same directory as where the stack config template is
        contained so that relative paths for resources can be relative to the stack config
        instead of where CLI calls the API functions.
    """
    config_path = _write_test_stack_config(tmpdir)
    config_working_dir = os.path.dirname(config_path)

    def _download(*args, **kwargs):
        assert os.getcwd() == config_working_dir

    stack_api_mock.download = mock.Mock(wraps=_download)
    runner = CliRunner()
    runner.invoke(cli.download, [config_path])


def test_load_json_config(tmpdir):
    """
        _load_json should read the same JSON content that was originally in the
        stack configuration JSON.
    """
    stack_path = os.path.join(tmpdir.strpath, TEST_STACK_PATH)
    os.makedirs(os.path.dirname(stack_path))
    with open(stack_path, "w+") as f:
        json.dump(TEST_STACK, f)
    config = cli._load_json(stack_path)
    assert config == TEST_STACK


def test_generate_stack_status_path(tmpdir):
    """
        The _generate_stack_status_path should add the word 'deployed' between the json file
        extension and the filename of the stack configuration file.
    """
    config_path = os.path.join(tmpdir.strpath, 'test.json')
    expected_status_path = os.path.join(tmpdir.strpath, 'test.deployed.json')
    generated_path = cli._generate_stack_status_path(config_path)
    assert expected_status_path == generated_path

    config_path = os.path.join(tmpdir.strpath, 'test.st-ack.json')
    expected_status_path = os.path.join(tmpdir.strpath, 'test.st-ack.deployed.json')
    generated_path = cli._generate_stack_status_path(config_path)
    assert expected_status_path == generated_path


def test_save_load_stack_status(tmpdir):
    """
        When saving the a stack status through _save_json, it should be able to be
        loaded by _load_json and have the same exact contents.
    """
    config_path = os.path.join(tmpdir.strpath, 'test.json')
    status_path = cli._generate_stack_status_path(config_path)
    cli._save_json(status_path, TEST_STATUS)

    status = cli._load_json(status_path)
    assert status == TEST_STATUS
