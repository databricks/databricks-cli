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

import re
import itertools
import copy
import click

from databricks_cli.utils import pretty_format


# Encode UTF-8 strings in JSON blobs
def mc_pretty_format(json):
    return pretty_format(json, encode_utf8=True)


def del_none(json):
    to_delete = []
    to_recurse = []
    for key, value in json.items():
        if value is None:
            to_delete.append(key)
        if value is dict:
            to_recurse.append(key)
    for key in to_delete:
        del json[key]
    for key in to_recurse:
        del_none(json[key])


def hide(cmd):
    """
    Return a copy of specified Click command instance with `hidden = True`.
    This requires Click >= v7.0.
    """
    cmd_copy = copy.copy(cmd)
    cmd_copy.hidden = True
    return cmd_copy


def json_file_help(method, path):
    path = "/api/2.0/unity-catalog" + path
    return "File containing JSON request to {} to {}.".format(method, path)


def json_string_help(method, path):
    path = "/api/2.0/unity-catalog" + path
    return "JSON string to {} to {}.".format(method, path)


def peek(iterable):
    try:
        first = next(iterable)
    except StopIteration:
        return None
    return itertools.chain([first], iterable)


spec_regex = r"(\w+)\s*(\=|like)\s*'([\w\d\s]+)'[,\s]*"
partition_regex = r"\((?:%s)+\)[,\s]*" % spec_regex

def parse_partitions(partition_spec):
    if partition_spec is None:
        return None

    partitions = []

    trimmed = partition_spec.strip()
    if len(trimmed) == 0:
        return partitions

    it = peek(re.finditer(partition_regex, trimmed, re.IGNORECASE))
    if it is None:
        raise click.BadParameter("bad partition specification format")

    for match in it:
        values = []
        partition = match.group()
        for (name, op, value) in re.findall(spec_regex, partition, re.IGNORECASE):
            values.append({
                'name': name,
                'op': 'EQUAL' if op == '=' else 'LIKE',
                'value': value,
            })
        partitions.append({
            'values': values,
        })
    return partitions