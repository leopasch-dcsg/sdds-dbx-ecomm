# How to Release to Production

Our projects utilize a two branch setup, the default branch is `main` and CI/CD is triggered to run `on push` to this branch.
For the `main` branch, we automatically deploy to DBX non-prod and QA.

For release to production, we need to merge `main` into the `release` branch. The release branch is configured to automatically
deploy `on push` to this branch.

## Steps to Release
* Make sure pyproject.toml `version` property is updated for the next release. We use the SEMVER standard for our bundles,
   MAJOR.MINOR.PATCH.
    * Use the following guidelines for incrementing the version:
        * MAJOR version when you make incompatible changes or any change that is not backwards compatible.
        * MINOR version when you add functionality in a backwards-compatible manner, typically look at the Jira tickets
          and anything that is a "User Story" is a candidate for increment the MINOR version by one.
        * PATCH version when you make backwards-compatible bug fixes.
* In `config/common/variables.yml` update the `bundle_version` property to match the same version as the `pyproject.toml` file.
* In `CHANGELOG.md` update the file to include all the changes being released in the bundle. Follow the same pattern as
  previous entries. The file is organized by bottom up, with the most recent release at the top.
* Create a tag on the main branch using the pattern `release_vMAJOR.MINOR.PATCH` and push this tag to the remote.
* Go into GitHub and create a release for the tag, you can just copy the entries from the CHANGELOG.md file into the GH release.
* Merge the `main` branch into the `release` branch to trigger the release.
  * Depending on current velocity of the team, you may want to cut a staging branch first before you merge into release
    to avoid merging incoming changes into the release branch. But this is probably an infrequent occurrence.
* Post an announcement to the [Production Changes](https://teams.microsoft.com/l/channel/19%3A215bac81cc804bb2a2b7de029983b071%40thread.tacv2/Production%20Changes?groupId=b758bdca-b096-4b77-8737-9c50ca8c722b&tenantId=e04b15c8-7a1e-4390-9b5b-28c7c205a233) channel in MS Teams.