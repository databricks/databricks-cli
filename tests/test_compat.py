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

import importlib
import inspect
import json
import os
import re
import sys
import tempfile
import unittest

import six


def normalize_module_name(name):
    """
    Replace `databricks_cli_main` with `databricks_cli` to make objects comparable.
    """
    return re.sub(r"^(databricks_cli)_main", r"\1", name)


def func_key(func):
    """
    Returns key of the full package path to the specified function.
    """
    return normalize_module_name(func.__module__) + "." + func.__qualname__


def collect_argspecs(modules):
    return {
        func_key(func): inspect.getfullargspec(func).args
        for module in modules
        for (_, clazz) in inspect.getmembers(module, predicate=inspect.isclass)
        for (_, func) in inspect.getmembers(clazz, predicate=inspect.isfunction)
        # Ignore functions that are defined outside the specified module.
        if module.__name__ == func.__module__
    }


# pylint: disable=superfluous-parens
def _test_compatibility(current_argspecs, existing_argspecs):
    removed_functions = []
    incompatible_functions = []
    for existing_name, existing_args in existing_argspecs.items():
        if existing_name not in current_argspecs:
            removed_functions.append(existing_name)
            continue

        # New arguments may only be appended to ensure compatibility
        # when functions are called with positional parameters.
        current_args = current_argspecs[existing_name]
        if (
            len(current_args) < len(existing_args)
            or current_args[0 : len(existing_args)] != existing_args
        ):
            incompatible_functions.append(existing_name)
            continue

    if removed_functions:
        print("Removed functions:")
        for fn in removed_functions:
            print(" * " + fn)
        assert len(removed_functions) == 0

    if incompatible_functions:
        print("Incompatible functions:")
        for fn in incompatible_functions:
            current_args = current_argspecs[fn]
            existing_args = existing_argspecs[fn]
            print(" * " + fn + ": " + str(existing_args) + " -> " + str(current_args))
        assert len(incompatible_functions) == 0


def import_databricks_modules(root):
    return [
        # Generated code.
        importlib.import_module(".sdk.service", root),
        # Functionality under the API package is used as an SDK by some.
        importlib.import_module(".cluster_policies.api", root),
        importlib.import_module(".clusters.api", root),
        importlib.import_module(".dbfs.api", root),
        importlib.import_module(".groups.api", root),
        importlib.import_module(".instance_pools.api", root),
        importlib.import_module(".jobs.api", root),
        importlib.import_module(".libraries.api", root),
        importlib.import_module(".pipelines.api", root),
        importlib.import_module(".repos.api", root),
        importlib.import_module(".runs.api", root),
        importlib.import_module(".secrets.api", root),
        importlib.import_module(".tokens.api", root),
        importlib.import_module(".unity_catalog.api", root),
        importlib.import_module(".workspace.api", root),
    ]


def databricks_cli_main_checkout_path():
    return os.getenv('DATABRICKS_CLI_MAIN_CHECKOUT')


def can_perform_compatibility_check():
    """
    Checks if the user has specified a path to a parallel checkout
    of the main branch of the databricks-cli repository.

    The files in this checkout will be imported under an aliased
    package name such that we have access to the argument specs
    of the functions in both the main branch and this repository.
    """
    path = databricks_cli_main_checkout_path()
    return path and os.path.isdir(path)


@unittest.skipIf(six.PY2 or not can_perform_compatibility_check(), reason=None)
def test_compatibility():
    """
    To run this test, checkout a reference copy of this repository and set
    the DATABRICKS_CLI_MAIN_CHECKOUT environment variable to its path.

    See `.github/workflows/` for the configuration of the GitHub action that runs it.
    """

    # Make the specified path importable by symlinking its `databricks_cli`
    # directory
    tmpdir = tempfile.mkdtemp()
    sys.path.append(tmpdir)
    os.symlink(
        os.path.join(databricks_cli_main_checkout_path(), "databricks_cli"),
        os.path.join(tmpdir, "databricks_cli_main"),
    )

    current_argspecs = collect_argspecs(import_databricks_modules("databricks_cli"))
    existing_argspecs = collect_argspecs(import_databricks_modules("databricks_cli_main"))
    _test_compatibility(current_argspecs, existing_argspecs)


if __name__ == '__main__':
    # If run directly, write the argspecs in this repository to stdout.
    json.dump(
        obj=collect_argspecs(import_databricks_modules("databricks_cli")),
        fp=sys.stdout,
        indent=2,
    )
