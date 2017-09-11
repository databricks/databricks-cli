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

import os
from base64 import b64encode, b64decode

import click

from databricks_cli.configure.config import get_workspace_client
from databricks_cli.dbfs.exceptions import LocalFileExistsException


DIRECTORY = 'DIRECTORY'
NOTEBOOK = 'NOTEBOOK'
LIBRARY = 'LIBRARY'


class WorkspaceFileInfo(object):
    def __init__(self, path, object_type, language=None):
        self.path = path
        self.object_type = object_type
        self.language = language

    def to_row(self, is_long_form, is_absolute):
        path = self.path if is_absolute else self.basename
        if self.is_dir:
            stylized_path = click.style(path, 'cyan')
        elif self.is_library:
            stylized_path = click.style(path, 'green')
        else:
            stylized_path = path
        if is_long_form:
            return [self.object_type, stylized_path, self.language]
        else:
            return [stylized_path]

    @property
    def is_dir(self):
        return self.object_type == DIRECTORY

    @property
    def is_notebook(self):
        return self.object_type == NOTEBOOK

    @property
    def is_library(self):
        return self.object_type == LIBRARY

    @property
    def basename(self):
        return os.path.basename(self.path)

    @classmethod
    def from_json(cls, deserialized_json):
        return cls(**deserialized_json)


def get_status(workspace_path):
    workspace_client = get_workspace_client()
    return WorkspaceFileInfo.from_json(workspace_client.get_status(workspace_path))


def list_objects(workspace_path):
    workspace_client = get_workspace_client()
    response = workspace_client.list(workspace_path)
    # This case is necessary when we list an empty dir in the workspace.
    # TODO(andrewmchen): We should make our API respond with a json with 'objects' field even
    # in this case.
    if 'objects' not in response:
        return []
    objects = response['objects']
    return [WorkspaceFileInfo.from_json(f) for f in objects]


def mkdirs(workspace_path):
    workspace_client = get_workspace_client()
    workspace_client.mkdirs(workspace_path)


def import_workspace(source_path, target_path, language, fmt, is_overwrite):
    workspace_client = get_workspace_client()
    with open(source_path, 'r') as f:
        content = b64encode(f.read())
        workspace_client.import_workspace(
            target_path,
            fmt,
            language,
            content,
            is_overwrite)


def export_workspace(source_path, target_path, fmt, is_overwrite):
    if os.path.exists(target_path) and not is_overwrite:
        raise LocalFileExistsException()
    workspace_client = get_workspace_client()
    output = workspace_client.export_workspace(source_path, fmt)
    content = output['content']
    # Will overwrite target_path.
    with open(target_path, 'wb') as f:
        decoded = b64decode(content)
        f.write(decoded)


def delete(workspace_path, is_recursive):
    workspace_client = get_workspace_client()
    workspace_client.delete(workspace_path, is_recursive)
