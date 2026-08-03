# E-Comm Data Engineering Overview
Below are the general guidelines and setup that all Data Engineering: E-Commerce projects should follow. This will drive
consistency across our data engineering projects allowing us to build and deliver high quality code that is readable,
maintainable, testable, and easily supported.

# Getting Started
Make sure you have a code editor installed. Most of the instructions below focus on configuring PyCharm, but you are
free to use any editor you prefer as long as you can configure the project and your IDE properly.

## Required Software
Ensure you have the following software installed before contributing to this project. For a full list of software that
is required for engineering on this team please see our [Required Software](https://dcsgcloud.atlassian.net/wiki/x/UQDrDw) confluence page.

1. An IDE (preferably PyCharm)
2. Java SDK (11 or 21), installed via self-service.
   * This is required to run unit tests locally because of PySpark
3. Homebrew, installed via self-service
4. Python 3.12.4 or greater, installed via homebrew
5. Databricks CLI, installed via homebrew
   * Used to deploy your local "target"
6. Docker, installed via self-service
   * Used to run migrations from your local machine

## Additional Getting Started Documentation
Please review the additional getting started documentation below before you contribute to this project.

* [Configure Environment Variables](documentation/environment-variables.md)
* [Configure Databricks CLI](documentation/databricks.md)
* [Configure PyCharm](documentation/pycharm.md)
* [Contributing Code](documentation/contributing.md)
* [Developing Notebooks](documentation/notebooks.md)
* [Developing and Testing Migrations](documentation/migrations.md)
* [Developing Automated Tests](documentation/testing.md)

## Release to Production
Please review the release documentation for running a release to production of our DBX E-Commerce project.

* [Release to Production](documentation/release_to_production.md)