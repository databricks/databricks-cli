# Databricks CLI
# Copyright 2022 Databricks, Inc.
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

import pytest

import databricks_cli.version as version


def test_is_release_version_defaults_to_current_version():
    # Not specifying a version explicitly implies testing the current version.
    assert version.is_release_version() == version.is_release_version(version.version)


def test_is_release_version():
    # Release versions
    assert version.is_release_version("0.16.0")
    assert version.is_release_version("0.16.8")
    assert version.is_release_version("0.16.1111")
    assert version.is_release_version("0.17.0")
    assert version.is_release_version("1.0.0")

    # Development versions
    assert not version.is_release_version("0.16.0.dev0")
    assert not version.is_release_version("0.16.8.dev0")
    assert not version.is_release_version("0.16.1111.dev1")
    assert not version.is_release_version("0.17.0.dev")
    assert not version.is_release_version("1.0.0.x")

    # Malformed version
    with pytest.raises(ValueError):
        version.is_release_version("foobar")

    with pytest.raises(ValueError):
        version.is_release_version("1.0.0dev0")

    with pytest.raises(ValueError):
        version.is_release_version("1.0.0.")

    with pytest.raises(ValueError):
        version.is_release_version("1.0.0.!")


def test_next_development_version():
    assert version.next_development_version("0.16.0") == "0.16.1.dev0"
    assert version.next_development_version("0.16.8") == "0.16.9.dev0"
    assert version.next_development_version("0.16.1111") == "0.16.1112.dev0"
    assert version.next_development_version("0.17.0") == "0.17.1.dev0"
    assert version.next_development_version("1.0.0") == "1.0.1.dev0"
