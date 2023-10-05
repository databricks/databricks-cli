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

import io
import os
from setuptools import setup, find_packages
from importlib.machinery import SourceFileLoader

path_to_module = os.path.join('databricks_cli', 'version.py')
loaded_module = SourceFileLoader('databricks_cli.version', path_to_module).load_module()
version = loaded_module.version

setup(
    name='databricks-cli',
    version=version,
    packages=find_packages(include=['databricks_cli*']),
    python_requires=">=3.7",
    install_requires=[
        # Note: please keep this in sync with `requirements.txt`.
        'click>=7.0',
        'pyjwt>=1.7.0',
        'oauthlib>=3.1.0',
        'requests>=2.17.3',
        'tabulate>=0.7.7',
        'six>=1.10.0',
        'urllib3>=1.26.7,<3'
    ],
    entry_points='''
        [console_scripts]
        databricks=databricks_cli.cli:main
        dbfs=databricks_cli.dbfs.cli:dbfs_group
    ''',
    zip_safe=False,
    author='Andrew Chen',
    author_email='andrewchen@databricks.com',
    description='A command line interface for Databricks',
    long_description=io.open('README.rst', encoding='utf-8').read(),
    license='Apache License 2.0',
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: Apache Software License',
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    keywords='databricks cli',
    url='https://github.com/databricks/databricks-cli',
    options={'bdist_wheel': {'universal': True}},
)
