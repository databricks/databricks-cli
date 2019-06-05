databricks-cli
==============
.. image:: https://travis-ci.org/databricks/databricks-cli.svg?branch=master
   :target: https://travis-ci.org/databricks/databricks-cli
   :alt: Build Status
.. image:: https://codecov.io/gh/databricks/databricks-cli/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/databricks/databricks-cli


The Databricks Command Line Interface (CLI) is an open source tool which provides an easy to use interface to
the Databricks platform. The CLI is built on top of the Databricks REST APIs.

**Note**: This CLI is under active development and is released as an experimental client. This means that interfaces are still subject to change.

If you're interested in contributing to the project please reach out.
In addition, please leave bug reports as issues on our `GitHub project <https://github.com/databricks/databricks-cli>`_.

Requirements
------------

-  Python Version > 2.7.9 or > 3.6

Installation
---------------

To install simply run
``pip install --upgrade databricks-cli``

Then set up authentication using username/password or `authentication token <https://docs.databricks.com/api/latest/authentication.html#token-management>`_. Credentials are stored at ``~/.databrickscfg``.

- ``databricks configure`` (enter hostname/username/password at prompt)
- ``databricks configure --token`` (enter hostname/auth-token at prompt)

Multiple connection profiles are also supported with ``databricks configure --profile <profile> [--token]``.
The connection profile can be used as such: ``databricks workspace ls --profile <profile>``.

To test that your authentication information is working, try a quick test like ``databricks workspace ls``.

Known Issues
---------------
``AttributeError: 'module' object has no attribute 'PROTOCOL_TLSv1_2'``

The Databricks web service requires clients speak TLSV1.2. The built in
version of Python for MacOS does not have this version of TLS built in.

To use the Databricks CLI you must install a version of Python that has ``ssl.PROTOCOL_TLSv1_2``.
For MacOS, the easiest way may be to install Python with `Homebrew <https://brew.sh/>`_.

Using Docker
------------
.. code::

    # build image
    docker build -t databricks-cli .

    # run container
    docker run -it databricks-cli

    # run command in docker
    docker run -it databricks-cli fs --help
    
Documentation
-------------

For the latest CLI documentation, see

- `Databricks <https://docs.databricks.com/user-guide/dev-tools/databricks-cli.html>`_
- `Azure Databricks <https://docs.azuredatabricks.net/user-guide/dev-tools/databricks-cli.html>`_
