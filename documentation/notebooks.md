# Developing Notebooks and Workflows
Databricks has support for developing notebooks in a few different ways. Options 1 and 2 are the preferred methods of
building the code that drives your workflows. Please avoid Jupyter notebooks.

1. Python Notebooks
    * These are generally python scripts that utilize "magic" comments to tell DBX your code should be treated as a notebook
2. Python Tasks
    * Python scripts, typically containing a main() method as the entry point. If developing a Python task, you should
      have a check to see if the script is being invoked on the main thread. See `ecmde_ecomm/common/notebooks/hash_key.py`
      as a working example.
      ```python
      # Typically this is placed at the bottom of your python script.
      if __name__ == "__main__":
          main()
      ```
3. Jupyter Notebooks (files with the `.ipynb` extension), I would typically avoid Jupyter notebooks as they require
   specific support in your IDE to develop, and contain more than just code. Please choose either "Python Notebooks" or
   or "Python Tasks".

## Source Location and Organization
* Notebooks should be organized into a python package named `notebooks` under the package for the data domain they 
  belong to.
  * Example: `src/ecmde_ecomm/search/notebooks`
* Reusable notebooks should live under `src/ecmde_ecomm/common/notebooks`

## Workflow Configuration
* All workflows (DBX jobs) should be defined in a YML file specific to the domain or task you are attempting to accomplish,
  basically keep co-related jobs and tasks together. Config files all live under `project_root/config/{domain}/file-name-jobs.yml`.
* Workflow configurations should be split into two separate files a `-jobs.yml` and a `-variables.yml` files.
  * The variables YML allows you to provide job specific variables. Please be sure to check if the `config/common/variables.yml`
    file already contains a defined variable you need. We don't want duplicates.
  * Make sure your variable names are unique, an easy way to do this is to prefix your job specific variables with a
    namespace value. Example: `ltr_webstore_key`.
  * Variable names are all lowercase.
* All variables are properly overridden for their deploy target in [databricks.yml](../databricks.yml)
* All jobs have the following YML blocks defined in [databricks.yml](../databricks.yml) for their production deployment
  target.
    ```yaml
      resources:
        jobs:
          ecmde-{domain}-your-job-name:
            email_notifications:
              on_failure:
                - TechPT-DataEcommerce@dcsg.com
                - TCS_DataAnalytics_SupportTeam@dcsg.com
              on_duration_warning_threshold_exceeded:
                - TechPT-DataEcommerce@dcsg.com
                - TCS_DataAnalytics_SupportTeam@dcsg.com
            health:
              rules:
                - metric: RUN_DURATION_SECONDS
                  op: GREATER_THAN
                  value: <pick a value that makes sense for your workflow>
    ```
* All jobs should use the following naming convention `ecmde-{domain}-{sub-domain}-descriptive-name`, all lowercase.
  * NOTE: sub-domain is optional, and should only be used if your core package is broken down into sub-domains.
* Task names should use the following name convention `descriptive-name`, all lowercase.
  * NOTE: task names just need to be descriptive to provide enough context as to their purpose.
* All task definitions contain the following libraries block (required for proper deployment) of our project.
  ```yaml
    libraries:
      - whl: ../../dist/*.whl
  ```

For more information on how to configure jobs, see the Databricks [Create Job](https://docs.databricks.com/api/azure/workspace/jobs/create) API documentation.
The examples provided are in JSON, but this does translate into YML quite easily (for the most part).

## Python Notebook
Python notebook tasks have a few rules you need to follow in order to make them work.

* The very first line of your python notebook needs to contain a "magic" comment that tells DBX this python source file
   should be treated like a notebook.
  ```python
  # Databricks notebook source
  ```
* The next two lines of your Notebook task should contain the following magic commands. This will load the autoreload 
  extension, which then auto reloads modules. Helpful with the way we deploy our project as a module. When jobs are 
  retried this will guarantee our project module gets auto reloaded.
  ```python
  # MAGIC %load_ext autoreload
  # MAGIC %autoreload 2
  ```
* Optional, but allows you to organce your notebook task into different cells. Use the "magic" command comment to break 
  your notebook up into logical sections (will only be evident in the DBX web UI).
  ```python
  # COMMAND ----------
  ```
* See DBX [Notebook Task API](https://docs.databricks.com/api/azure/workspace/jobs/create#tasks-notebook_task) for a full list of configuration options.

## Python Task
Python tasks are simpler in nature, but do require a little more configuration to work. See [callable-jobs.yml](../config/common/callable-jobs.yml) and [[project.scripts]](../pyproject.toml) in pyproject.toml for a full working example.