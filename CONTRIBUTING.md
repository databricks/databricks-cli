Setting up the dev environment
---------------------------------
To isolate dependencies from your global python installation, it is important to use a tool like
[virtualenv](https://virtualenv.pypa.io/en/stable/). With `virtualenv` you can install the dev environment by doing the following.

Another option would be to use docker directly, i.e. 
```bash
docker run -it -v `echo $PWD`:/root python:3.6.8 bash
docker run -it -v `echo $PWD`:/root python:2.7.12 bash

```

- `pip install -e .`
- `pip install -r dev-requirements.txt`
- `pip install -r tox-requirements.txt`

To verify that the installation of `databricks-cli` is the one checked out from VCS, you can check by doing `python -c "import databricks_cli; print databricks_cli.__file__"`.

Running Tests
----------------
- `tox`

