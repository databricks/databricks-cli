# Examples using Databricks Stack CLI

Here we provide examples on how to use Databricks Stack CLI to manage Databricks resource stacks.

## Example: notebook-job-project

In this example we deploy a notebook to Azure Databricks workspace and create a job for the notebook.

### Commands:

* Go to the corresponding project directory:

```
cd notebook-job-project
```

* Deploy the stack configuration:

```
databricks stack deploy --profile $PROFILE project-stack-config.json -o
```

After the stack is successfully deployed, a stack status file `project-stack-config.deployed.json` will be created in the same directory.
