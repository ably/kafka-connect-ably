# Upgrade / Migration Guide

## Version 2.x to 3.0

There are several **breaking changes** to configuration in the Ably Connector v3.0.0, due to improvements made and
historical configuration being superseded. The summary below covers the main changes:

* **Migration from Realtime (WebSocket) to REST Publishing.** The connector now uses REST to publish records to Ably
  with parallel publishing using a thread pool for improved throughput per sink task. All configuration that related
  only to Realtime WebSocket connections have been removed and can be safely dropped from config files as they would
  no-longer have any affect.
* **Message Ordering** is no-longer preserved by default. Users will need to set `batchExecutionThreadPoolSize=1` to
  disable parallel publishing from each sink task to avoid messages arriving at Ably in a different order to the way
  they were published to the Kafka Topic Partition. Parallelism can only be achieved at the sink task level if message
  ordering must be preserved, by:
    * Ensuring that the Topic has sufficiently many partitions for parallel consumption
    * Setting the connector `max.tasks` value to the desired level of parallelism (not more than the number of topic partitions)
    * Setting `batchExecutionThreadPoolSize=1` to avoid parallelism within each task.
* `skipKeyOnAbsense` configuration has been replaced by the more flexible `onFailedRecordMapping` configuration. See
  [Handling Failed Mappings](/README.md#handling-failed-mappings) for more information.

## Version 1.0.3 to 2.0.0

We have made some **breaking changes** in the version 2.0.0 release of this project. Configurations below are no longer supported.
You must remove these from your configuration files when using the new version.

* `recover`
* `client.use.binary.protocol`

Also;
* Message name default value is no longer 'sink'. You must set new configuration `message.name` or it is going to be set to null.
