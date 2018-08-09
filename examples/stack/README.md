# Stack CLI Quick Start
This example goes through deploying a stack containing a notebook and a job to a Databricks workspace through the Stack CLI (`databricks stack`).
The "Hello, world" Notebook will go into the `Shared` directory of the workspace and a job will be created
(or updated) on deploy.

The example commands shown is for the `sample-aws-stack-nb.json` file which is only compatible with an
AWS workspace. For an azure workspace, use the `sample-azure-stack-nb.json` file in the commands instead.

Note: The Databricks Stack CLI is still in beta, so command names or JSON fields are subject to change.

## Setup
### Setup Authentication (skip if authentication already set up)
Enter the following with with the desired name of the workspace `<profile_name>`:
```
$ databricks configure --token --profile <profile_name>
```
(enter the hostname and auth-token at prompt)

### Download Repo, Navigate to Example Folder
```
$ git clone https://github.com/databricks/databricks-cli.git
$ cd databricks-cli/examples/stack/quickstart
```

## Initial Deployment of Stack
To deploy the stack to a workspace authenticated under the profile, `<profile_name>`, run this command:
```
$ databricks stack deploy --profile <profile_name> sample-aws-stack-nb.json
```

## Download Changes of Stack from Workspace
If you have made changes to the notebook on your workspace and want to download these changes locally,
run this command:
```
$ databricks stack download --profile <profile_name> --overwrite sample-aws-stack-nb.json
```
## Update Stack
If you have made changes to the notebook source code or job configurations locally, you can
run this command to update the stack on your workspace.
```
$ databricks stack deploy --profile <profile_name> --overwrite sample-aws-stack-nb.json
```
