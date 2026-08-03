# Configure Databricks
In order to develop and test your notebooks, you will need to be able to deploy your asset bundle using the Databricks CLI.

## Install and Configure Databricks CLI (preferred)
1. [Install CLI via Homebrew](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/install#homebrew-install)
2. [Create Databricks Configuration Profile](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/config-profiles)
    * DEV Host: https://adb-7098146655993014.14.azuredatabricks.net
3. Authenticate using the CLI with your Databricks profile
    ```shell
      # NOTE: if you have more than on profile you will be asked which profile name you want to use.
      databricks auth login
    ```

### Example `.databrickscfg` File
If you've configured the CLI right, you should have a `.databrickscfg` file in your home folder that looks like the
example below.

```text
[DEFAULT]
host      = https://adb-7098146655993014.14.azuredatabricks.net
auth_type = databricks-cli
```