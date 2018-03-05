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

from databricks_cli.workspace import cli
from tests.utils import invoke_cli_runner

WORKSPACE_TEST_PATH = '/databricks-cli-test' + str(int(random() * 1000))
LOCAL_TEMP_FILE = 'temp-file.txt'
LOCAL_TEMP_DIR = 'temp-dir'

SCALA_FILE = 'test-a.scala'
SQL_FILE = 'test-b.sql'
PYTHON_FILE = 'test-c.py'
R_FILE = 'test-d.R'

SCALA_CONTENTS = "println(1+1)"
SQL_CONTENTS = "select 1+1"
PYTHON_CONTENTS = "print 1+1"
R_CONTENTS = "print(1+1)"

LOCAL_TEST_DIR = 'test-dir'


@pytest.fixture()
def workspace_dir():
    invoke_cli_runner(cli.mkdirs_cli, [WORKSPACE_TEST_PATH])
    yield
    invoke_cli_runner(cli.delete_cli, ['-r', WORKSPACE_TEST_PATH])


@pytest.fixture()
def local_dir(tmpdir):
    """
    Creates a tmpdir with this structure.
    - tmpdir
      - test.scala
      - test-dir
        - test.scala
        - test.sql
        - test.py
        - test.R
    """
    path = tmpdir.strpath
    with open(os.path.join(path, SCALA_FILE), 'wt') as f:
        f.write(SCALA_CONTENTS)
    os.mkdir(os.path.join(path, LOCAL_TEST_DIR))
    with open(os.path.join(path, os.path.join(LOCAL_TEST_DIR, SCALA_FILE)), 'wt') as f:
        f.write(SCALA_CONTENTS)
    with open(os.path.join(path, os.path.join(LOCAL_TEST_DIR, SQL_FILE)), 'wt') as f:
        f.write(SQL_CONTENTS)
    with open(os.path.join(path, os.path.join(LOCAL_TEST_DIR, PYTHON_FILE)), 'wt') as f:
        f.write(PYTHON_CONTENTS)
    with open(os.path.join(path, os.path.join(LOCAL_TEST_DIR, R_FILE)), 'wt') as f:
        f.write(R_CONTENTS)
    yield tmpdir


def assert_local_file_contains(local_path, expected_contents):
    assert os.path.exists(local_path)
    with open(local_path, 'rt') as f:
        assert expected_contents in f.read()


def assert_workspace_file_exists(workspace_path):
    dirname, basename = os.path.split(workspace_path)
    res = invoke_cli_runner(cli.ls_cli, [dirname])
    assert basename in res.output.split("\n")


def strip_suffix(filename):
    return os.path.splitext(filename)[0]


class TestWorkspaceCli(object):
    @pytest.mark.usefixtures('workspace_dir')
    def test_mkdirs(self):
        pass

    @pytest.mark.usefixtures('workspace_dir')
    def test_ls(self):
        assert_workspace_file_exists(WORKSPACE_TEST_PATH)

    @pytest.mark.usefixtures('workspace_dir')
    def test_import_export(self, local_dir):
        path = local_dir.strpath
        invoke_cli_runner(cli.import_workspace_cli, [os.path.join(path, SCALA_FILE),
                                                     os.path.join(WORKSPACE_TEST_PATH, SCALA_FILE),
                                                     "-l", "scala"])
        assert_workspace_file_exists(os.path.join(WORKSPACE_TEST_PATH, SCALA_FILE))
        invoke_cli_runner(cli.export_workspace_cli, [os.path.join(WORKSPACE_TEST_PATH, SCALA_FILE),
                                                     os.path.join(path, LOCAL_TEMP_FILE)])
        assert_local_file_contains(os.path.join(path, LOCAL_TEMP_FILE), SCALA_CONTENTS)

    @pytest.mark.usefixtures('workspace_dir')
    def test_import_export_recursive(self, local_dir):
        path = local_dir.strpath
        os.chdir(path)
        invoke_cli_runner(cli.import_dir_cli, ['-o', '.', WORKSPACE_TEST_PATH])
        assert_workspace_file_exists(os.path.join(WORKSPACE_TEST_PATH, strip_suffix(SCALA_FILE)))

        for f in [SCALA_FILE, SQL_FILE, PYTHON_FILE, R_FILE]:
            remote_path = os.path.join(WORKSPACE_TEST_PATH, os.path.join(LOCAL_TEST_DIR,
                                                                         strip_suffix(f)))
            assert_workspace_file_exists(remote_path)

        # Copy the data back to `temp-dir`.
        local_temp_dir = os.path.join(path, LOCAL_TEMP_DIR)
        invoke_cli_runner(cli.export_dir_cli, [WORKSPACE_TEST_PATH, local_temp_dir])
        assert_local_file_contains(os.path.join(local_temp_dir, SCALA_FILE), SCALA_CONTENTS)
        for f, content in [(SCALA_FILE, SCALA_CONTENTS), (SQL_FILE, SQL_CONTENTS),
                           (PYTHON_FILE, PYTHON_CONTENTS), (R_FILE, R_CONTENTS)]:
            local_path = os.path.join(local_temp_dir, os.path.join(LOCAL_TEST_DIR, f))
            assert_local_file_contains(local_path, content)
