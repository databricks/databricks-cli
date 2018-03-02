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

import os
from random import random

import pytest

from databricks_cli.dbfs import cli
from databricks_cli.dbfs.dbfs_path import DbfsPath
from tests.utils import invoke_cli_runner

DBFS_TEST_PATH = 'dbfs:/databricks-cli-test-' + str(int(random() * 1000))
LOCAL_TEMP_FILE = 'temp-file.txt'
LOCAL_TEMP_DIR = 'temp-dir'
LOCAL_TEST_FILE = 'test-file.txt'
LOCAL_TEST_DIR = 'test-dir'
LOCAL_TEST_FILE_IN_DIR = os.path.join(LOCAL_TEST_DIR, LOCAL_TEST_FILE)
TEST_FILE_CONTENTS = 'Hi I am a test file.\n'


@pytest.fixture()
def dbfs_dir():
    invoke_cli_runner(cli.mkdirs_cli, [DBFS_TEST_PATH])
    yield
    invoke_cli_runner(cli.rm_cli, ['-r', DBFS_TEST_PATH])


@pytest.fixture()
def local_dir(tmpdir):
    """
    Creates a tmpdir with this structure.
    - tmpdir
      - test-file.txt
      - test-dir
        - test-file.txt
    """
    path = tmpdir.strpath
    with open(os.path.join(path, LOCAL_TEST_FILE), 'wt') as f:
        f.write(TEST_FILE_CONTENTS)
    os.mkdir(os.path.join(path, LOCAL_TEST_DIR))
    with open(os.path.join(path, LOCAL_TEST_FILE_IN_DIR), 'wt') as f:
        f.write(TEST_FILE_CONTENTS)
    yield tmpdir


def assert_local_file_content(local_path, expected_contents):
    assert os.path.exists(local_path)
    with open(local_path, 'rt') as f:
        assert f.read() == expected_contents


def assert_dbfs_file_exists(dbfs_path):
    basename = dbfs_path.basename
    dirname = dbfs_path.absolute_path[:-len(basename)]
    res = invoke_cli_runner(cli.ls_cli, [dirname])
    files = res.output.split('\n')
    assert basename in files


class TestDbfsCli(object):
    @pytest.mark.usefixtures('dbfs_dir')
    def test_mkdirs(self):
        pass

    @pytest.mark.usefixtures('dbfs_dir')
    def test_ls(self):
        assert_dbfs_file_exists(DbfsPath(DBFS_TEST_PATH))

    @pytest.mark.usefixtures('dbfs_dir')
    def test_cp_from_local(self, local_dir):
        path = local_dir.strpath
        invoke_cli_runner(cli.cp_cli, [os.path.join(path, LOCAL_TEST_FILE), DBFS_TEST_PATH])
        assert_dbfs_file_exists(DbfsPath(DBFS_TEST_PATH).join(LOCAL_TEST_FILE))

    @pytest.mark.usefixtures('dbfs_dir')
    def test_cp_from_remote(self, local_dir):
        path = local_dir.strpath
        invoke_cli_runner(cli.cp_cli, [os.path.join(path, LOCAL_TEST_FILE), DBFS_TEST_PATH])

        temp_file_path = os.path.join(path, LOCAL_TEMP_FILE)
        invoke_cli_runner(cli.cp_cli, [os.path.join(DBFS_TEST_PATH, LOCAL_TEST_FILE),
                                       temp_file_path])

        assert_local_file_content(temp_file_path, TEST_FILE_CONTENTS)

    @pytest.mark.usefixtures('dbfs_dir')
    def test_cp_recursive(self, local_dir):
        path = local_dir.strpath
        os.chdir(path)
        invoke_cli_runner(cli.cp_cli, ['-r', '.', DBFS_TEST_PATH])
        assert_dbfs_file_exists(DbfsPath(DBFS_TEST_PATH).join(LOCAL_TEST_FILE))
        assert_dbfs_file_exists(DbfsPath(DBFS_TEST_PATH).join(LOCAL_TEST_DIR))
        assert_dbfs_file_exists(DbfsPath(DBFS_TEST_PATH).join(LOCAL_TEST_FILE_IN_DIR))

        # Copy the data back to `temp-dir`.
        local_temp_dir = os.path.join(path, LOCAL_TEMP_DIR)
        invoke_cli_runner(cli.cp_cli, ['-r', DBFS_TEST_PATH, local_temp_dir])
        assert_local_file_content(os.path.join(local_temp_dir, LOCAL_TEST_FILE), TEST_FILE_CONTENTS)
        assert_local_file_content(os.path.join(local_temp_dir, LOCAL_TEST_FILE_IN_DIR),
                                  TEST_FILE_CONTENTS)
