Setting up the dev environment
---------------------------------
To isolate dependencies from your global python installation, it is important to use a tool like
[virtualenv](https://virtualenv.pypa.io/en/stable/). With `virtualenv` you can install the dev environment by doing the following.

Another option would be to use docker directly, i.e.

```bash
docker run -it -v `echo $PWD`:/root python:3.7 bash
```

- `pip install -e .`
- `pip install -r dev-requirements.txt`

To verify that the installation of `databricks-cli` is the one checked out from VCS, you can check by doing `python -c "import databricks_cli; print(databricks_cli.__file__)"`.

Developing using VSCode dev containers
--------------------------------------

This repo comes pre-configured with a devolpment container for the VSCode [Remote Containers](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) extension. When opening this project in VSCode you will be asked if you want to open it in a dev container. Click yes and VSCode will build a docker container which everything needed to develop the Databricks CLI and attach VSCode to the container.

Requirements:

1. VSCode with the remote containers extension installed
2. A working docker installation

Developing using Github CodeSpaces
----------------------------------

The same development container setup used for local VSCode also works with GitHub CodeSpaces. If you have CodeSpaces enabled in your Github account then can just create a CodeSpace from the repoand start coding.

In order to test the CLI against a Databricks cluster you can define the these secrets for your CodeSpace so you don't have to run `databricks init` eacht time you open it:

- `DATABRICKS_HOST`: Workspace URL
- `DATABRICKS_TOKEN`: Personal access token

https://docs.github.com/en/codespaces/managing-your-codespaces/managing-encrypted-secrets-for-your-codespaces


Running Tests
----------------
- `tox`
