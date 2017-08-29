databricks-cli
==============
.. image:: https://travis-ci.org/databricks/databricks-cli.svg?branch=master
   :target: https://travis-ci.org/databricks/databricks-cli
   :alt: Build Status


This repository includes the code for the command line interface to
Databricks APIs. Currently, the only APIs implemented are for DBFS.
**PLEASE NOTE**, this CLI is under active development and is released as
an experimental client. This
means that interfaces are subject to being changed and that
SLAs/engineering support are not provided.

If you're interested in contributing to the project please reach out.
In addition, please leave bug reports as issues on our `Github project <https://github.com/databricks/databricks-cli>`_.

Requirements
------------

-  Python Version > 2.7.9
-  Python 3 is not supported

Installation
---------------

To install simply run
``pip install databricks-cli``

In order to upgrade your databricks-cli installation please run
``pip install --upgrade databricks-cli``

Getting started and setting up authentication
----------------------------------------------

After installing, ``databricks`` and ``dbfs`` (shorthand for ``databricks fs``) will be installed
into your PATH. Try it out by running ``dbfs --help``.

There are two ways to authenticate to the Databricks API. The first way
is to use your username and password pair. To do this run ``dbfs configure``
and follow the prompts. The second way is to use a access token generated inside of
Databricks. To configure the CLI to use an access token run ``dbfs configure --token``
and follow the prompts.

Known Issues
---------------
``AttributeError: 'module' object has no attribute 'PROTOCOL_TLSv1_2'``

For compliance reasons, our webapp requires the client to speak TLSV1.2. The built in
version of Python for MacOS does not have this version of TLS built in.

To use databricks-cli you should install a version of Python which has ``ssl.PROTOCOL_TLSv1_2``.
For MacOS, the easiest way may be to install Python with `Homebrew <https://brew.sh/>`_.


Don't have a password because of SSO?
-------------------------------------

Databricks will soon provide a token service which will allow users to authenticate to the API
using a secret token.
