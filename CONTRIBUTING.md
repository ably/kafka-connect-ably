# Contributing

## Development Flow

1. Fork the GitHub repository and clone it to your local machine
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Ensure you have added suitable tests and the test suite is passing (see [Running Tests](#running-tests) below)
5. Push the feature branch to GitHub (`git push origin my-new-feature`)
6. Create a new Pull Request

### Building

Build the connector using [Gradle](https://gradle.org/):

    ./gradlew clean assemble

## Running Tests

There are both unit tests and integration tests that can be run using [Gradle](https://gradle.org/).

Run the unit tests:

    ./gradlew unitTest

Run the integration tests:

    ./gradlew test

## Release Process

This library uses [semantic versioning](http://semver.org/). For each release, the following needs to be done:

1. Create a branch for the release, named like `release/1.2.3` (where `1.2.3` is the new version you want to release)
2. Update the version number in the `product.version` element of [`pom.xml`](./pom.xml)
3. Run [`github_changelog_generator`](https://github.com/github-changelog-generator/github-changelog-generator) to automate the update of the [CHANGELOG](./CHANGELOG.md). This may require some manual intervention, both in terms of how the command is run and how the change log file is modified. Your mileage may vary:
  * The command you will need to run will look something like this: `github_changelog_generator -u ably -p kafka-connect-ably --since-tag v1.2.2 --output delta.md` (where `1.2.2` is the version number of the previous release)
  * Using the command above, `--output delta.md` writes changes made after `--since-tag` to a new file
  * The contents of that new file (`delta.md`) then need to be manually inserted at the top of the `CHANGELOG.md`, changing the "Unreleased" heading and linking with the current version numbers
  * Also ensure that the "Full Changelog" link points to the new version tag instead of the `HEAD`
  * Commit this change: `git add CHANGELOG.md && git commit -m "Update change log."`
4. Make a PR against `main`
5. Once the PR is approved, merge it into `main`
6. Add a tag and push to origin - e.g.: `git tag v1.2.3 && git push origin v1.2.3`
7. Create the release on Github including populating the release notes
8. Notify the product team that a new ZIP file needs to be sent to Confluent Hub, which can be downloaded at the following URL (replacing the version number with the version number built, available once [the workflow](.github/workflows/integration-test.yml) has finished executing on `main`): https://sdk.ably.com/builds/ably/kafka-connect-ably/main/kafka-connect-ably/ably-kafka-connect-ably-1.2.3.zip
