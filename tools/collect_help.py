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

import os
import click

from databricks_cli.cli import cli


def recurse(obj, dry_run, prefix):
    """
    For each command in the specified object, write `--help` output to a file.
    Recurse into nested groups and commands.
    """
    if not dry_run:
        try:
            os.makedirs('/'.join(prefix))
        except FileExistsError:
            pass
    for name, command in obj.commands.items():
        if command.hidden:
            continue

        path = '/'.join(prefix + [name])
        if isinstance(command, click.Group):
            recurse(command, dry_run, prefix + [name])
        if isinstance(command, click.Command):
            path = path + '.txt'
            print('Write ' + path)
            if not dry_run:
                with open(path, 'w', encoding='utf-8') as file:
                    with click.Context(command) as ctx:
                        file.write(command.get_help(ctx))


@click.command()
@click.option('--dry-run', is_flag=True, show_default=True, default=False)
@click.option('--prefix', metavar='PATH', show_default=True, default='./help')
def main(dry_run, prefix):
    """
    Generate `--help` output for each command in the Databricks CLI.
    """
    recurse(cli, dry_run, prefix=[prefix])


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
