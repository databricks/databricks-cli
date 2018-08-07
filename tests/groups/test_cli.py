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

import mock
import pytest
from click.testing import CliRunner

from databricks_cli.groups import cli
from databricks_cli.utils import pretty_format
from tests.utils import provide_conf

CREATE_RETURN = {"group_id": "test"}
TEST_GROUP = "test_group"
TEST_PARENT_GROUP = "test_parent_group"
TEST_USER = "test_user"
GROUPS = {
    "group_names": [
        TEST_GROUP,
        TEST_PARENT_GROUP
    ]
}
GROUP_MEMBERS = {
    "members": [
        {
            "user_name": TEST_USER
        }
    ]
}
GROUP_PARENTS = {
    "group_names": [
        TEST_PARENT_GROUP
    ]
}


@pytest.fixture()
def group_api_mock():
    with mock.patch('databricks_cli.groups.cli.GroupsApi') as mocked:
        _group_api_mock = mock.MagicMock()
        mocked.return_value = _group_api_mock
        yield _group_api_mock


@provide_conf
def test_add_member_validation(group_api_mock):
    runner = CliRunner()
    runner.invoke(cli.add_member_cli,
                  ["--parent-name", TEST_PARENT_GROUP,
                   "--user-name", TEST_USER,
                   "--group-name", TEST_GROUP])
    assert group_api_mock.add_member.call_count == 0


@provide_conf
def test_add_user_to_group(group_api_mock):
    runner = CliRunner()
    runner.invoke(cli.add_member_cli,
                  ["--parent-name", TEST_PARENT_GROUP,
                   "--user-name", TEST_USER])
    group_api_mock.add_member.assert_called_once_with(
        parent_name=TEST_PARENT_GROUP,
        user_name=TEST_USER,
        group_name=None
    )


@provide_conf
def test_add_group_to_parent_group(group_api_mock):
    runner = CliRunner()
    runner.invoke(cli.add_member_cli,
                  ["--parent-name", TEST_PARENT_GROUP,
                   "--group-name", TEST_GROUP])
    group_api_mock.add_member.assert_called_once_with(
        parent_name=TEST_PARENT_GROUP,
        user_name=None,
        group_name=TEST_GROUP,
    )


@provide_conf
def test_create_cli(group_api_mock):
    runner = CliRunner()
    runner.invoke(cli.create_cli, ["--group-name", TEST_GROUP])
    group_api_mock.create.assert_called_once_with(TEST_GROUP)


@provide_conf
def test_list_members_cli(group_api_mock):
    with mock.patch('databricks_cli.groups.cli.click.echo') as echo_mock:
        group_api_mock.list_members.return_value = GROUP_MEMBERS
        runner = CliRunner()
        runner.invoke(cli.list_members_cli, ["--group-name", TEST_GROUP])
        group_api_mock.list_members.assert_called_once_with(TEST_GROUP)
        echo_mock.assert_called_once_with(pretty_format(GROUP_MEMBERS))


@provide_conf
def test_list_all_cli(group_api_mock):
    with mock.patch('databricks_cli.groups.cli.click.echo') as echo_mock:
        group_api_mock.list_all.return_value = GROUPS
        runner = CliRunner()
        runner.invoke(cli.list_all_cli)
        group_api_mock.list_all.assert_called_once()
        echo_mock.assert_called_once_with(pretty_format(GROUPS))


@provide_conf
def test_list_group_parents_cli(group_api_mock):
    with mock.patch('databricks_cli.groups.cli.click.echo') as echo_mock:
        group_api_mock.list_parents.return_value = GROUP_PARENTS
        runner = CliRunner()
        runner.invoke(cli.list_parents_cli, ['--group-name', TEST_GROUP])
        group_api_mock.list_parents.assert_called_once_with(user_name=None, group_name=TEST_GROUP)
        echo_mock.assert_called_once_with(pretty_format(GROUP_PARENTS))


@provide_conf
def test_list_user_groups_cli(group_api_mock):
    with mock.patch('databricks_cli.groups.cli.click.echo') as echo_mock:
        group_api_mock.list_parents.return_value = GROUPS
        runner = CliRunner()
        runner.invoke(cli.list_parents_cli, ['--user-name', TEST_USER])
        group_api_mock.list_parents.assert_called_once_with(user_name=TEST_USER, group_name=None)
        echo_mock.assert_called_once_with(pretty_format(GROUPS))


@provide_conf
def test_remove_user_from_group_cli(group_api_mock):
    runner = CliRunner()
    runner.invoke(cli.remove_member_cli, ["--parent-name", TEST_PARENT_GROUP,
                                          "--user-name", TEST_USER])
    group_api_mock.remove_member.assert_called_once_with(
        parent_name=TEST_PARENT_GROUP,
        user_name=TEST_USER,
        group_name=None,
    )


@provide_conf
def test_remove_group_from_parent_cli(group_api_mock):
    runner = CliRunner()
    runner.invoke(cli.remove_member_cli, ["--parent-name", TEST_PARENT_GROUP,
                                          "--group-name", TEST_GROUP])
    group_api_mock.remove_member.assert_called_once_with(
        parent_name=TEST_PARENT_GROUP,
        user_name=None,
        group_name=TEST_GROUP,
    )


@provide_conf
def test_delete_cli(group_api_mock):
    runner = CliRunner()
    runner.invoke(cli.delete_cli, ["--group-name", TEST_GROUP])
    group_api_mock.delete.assert_called_once_with(TEST_GROUP)
