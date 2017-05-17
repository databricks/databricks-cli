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

Getting started
----------------

After installing, ``dbfs`` will be installed into your PATH. Try it out
by running ``dbfs --help``.

To configure your username/password/host try running ``dbfs configure``.
You will be prompted for your username, password, and host.

Known Issues
---------------
``AttributeError: 'module' object has no attribute 'PROTOCOL_TLSv1_2'``

For compliance reasons, our webapp requires the client to speak TLSV1.2. The built in
version of Python for MacOS does not have this version of TLS built in.

To use databricks-cli you should install a version of Python which has ``ssl.PROTOCOL_TLSv1_2``.
For MacOS, the easiest way may be to install Python with `Homebrew <https://brew.sh/>`_.


Don't have a password because of SSO?
-------------------------------------

Your administrator can choose to set a password for you.
