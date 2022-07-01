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

import inspect
import json
import os

import databricks_cli.sdk.service

import databricks_cli.cluster_policies.api
import databricks_cli.clusters.api
import databricks_cli.dbfs.api
import databricks_cli.groups.api
import databricks_cli.instance_pools.api
import databricks_cli.jobs.api
import databricks_cli.libraries.api
import databricks_cli.pipelines.api
import databricks_cli.repos.api
import databricks_cli.runs.api
import databricks_cli.secrets.api
import databricks_cli.tokens.api
import databricks_cli.unity_catalog.api
import databricks_cli.workspace.api


def collect_argspecs(modules):
    return {
        func.__module__ + "." + func.__qualname__: inspect.getfullargspec(func).args
        for mod in modules
        for (_, clazz) in inspect.getmembers(mod, predicate=inspect.isclass)
        for (_, func) in inspect.getmembers(clazz, predicate=inspect.isfunction)
    }


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


def test_compatibility():
    api_packages = [
        databricks_cli.cluster_policies.api,
        databricks_cli.clusters.api,
        databricks_cli.dbfs.api,
        databricks_cli.groups.api,
        databricks_cli.instance_pools.api,
        databricks_cli.jobs.api,
        databricks_cli.libraries.api,
        databricks_cli.pipelines.api,
        databricks_cli.repos.api,
        databricks_cli.runs.api,
        databricks_cli.secrets.api,
        databricks_cli.tokens.api,
        databricks_cli.unity_catalog.api,
        databricks_cli.workspace.api,
    ]

    current_argspecs = collect_argspecs([databricks_cli.sdk.service] + api_packages)

    test_compatibility_file = os.getenv('TEST_COMPATIBILITY_FILE', default=None)
    if test_compatibility_file:
        with open(test_compatibility_file, 'r', encoding='utf-8') as f:
            existing_argspecs = json.load(f)
        _test_compatibility(current_argspecs, existing_argspecs)
    else:
        # Write argspecs to file.
        test_compatibility_file = os.path.join(os.path.dirname(__file__), "test_compat.json")
        with open(test_compatibility_file, 'w', encoding='utf-8') as f:
            json.dump(current_argspecs, f, indent=2)
