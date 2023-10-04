import warnings

warnings.warn("the databricks-cli module is deprecated in favor of databricks-sdk-py. Python 3.12 will be the last "
              "version of Python supported by databricks-cli. Please migrate to databricks-sdk-py as documented in "
              "the migration guide: https://docs.databricks.com/en/dev-tools/cli/migrate.html",
              DeprecationWarning, stacklevel=2)
