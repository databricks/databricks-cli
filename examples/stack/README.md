# Stack CLI Example
This Example goes through deploying a stack containing a notebook and a job to a Databricks workspace.
The "Hello, world" Notebook will go into the `Shared` directory of the workspace and a job will be created
(or updated) on deploy.
## Setup
### Setup Authentication (skip if authentication already set up)
Enter the following with with the desired name of the workspace `<profile_name>`:
```
$ databricks configure --token --profile <profile_name>
```
(enter hostname and auth-token at prompt)

### Download Repo, Navigate to Example Folder
```
$ git clone https://github.com/databricks/databricks-cli.git
$ cd databricks-cli/examples/stack
```

## Initial deployment of Stack
To deploy the stack to a workspace authenticated under the profile, `<profile_name>`, run this command:
```
$ stack_cli deploy --profile <profile_name> sample-aws-stack-nb.json
```

## Download Changes of Pipeline from Databricks
If you have made changes to the notebook on workspace and want to download these changes locally,
run this command:
```
$ stack_cli download --profile <profile_name> --overwrite sample-aws-stack-nb.json
```

## Update Deployment
If you have made changes to the notebook source code or job configurations locally, you can
run this command to update the stack on your workspace.
```
$ stack_cli deploy --profile <profile_name> --overwrite sample-aws-stack-nb.json
```
