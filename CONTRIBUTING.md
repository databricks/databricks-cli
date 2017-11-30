Setting up the dev environment
---------------------------------
To isolate dependencies from your global python installation, it is important to use a tool like
[virtualenv](https://virtualenv.pypa.io/en/stable/). With `virtualenv` you can install the dev environment by doing the following.

- `pip install -e .`
- `pip install -r dev-requirements.txt`

To verify that the installation of `databricks-cli` is the one checked out from VCS, you can check by doing `python -c "import databricks_cli; print databricks_cli.__file__"`.

Running Tests
----------------
- `pytest tests`

