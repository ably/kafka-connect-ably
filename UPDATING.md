# Upgrade / Migration Guide

## Version 1.0.3 to 2.0.0

We have made some **breaking changes** in the version 2.0.0 release of this project. Configurations below are no longer supported.
You must remove these from your configuration files when using the new version.

* `recover`
* `client.use.binary.protocol`

Also;
* Message name default value is no longer 'sink'. You must set new configuration `message.name` or it is going to be set to null.
