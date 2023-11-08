# Ably Kafka Connector

_[Ably](https://ably.com) is a realtime experiences infrastructure platform. Developers use our SDKs and APIs to build, extend and deliver realtime capabilities like Chat, Data Broadcast, Data Synchronization, Multiplayer Collaboration, and Notifications. Each month, Ably powers digital experiences in realtime for more than 350 million devices across 80 countries each. Organizations like HubSpot, Genius Sports, Verizon, Webflow, and Mentimeter depend on Ably’s platform to offload the growing complexity of business-critical realtime infrastructure at a global scale. For more information, see the [Ably documentation](https://ably.com/documentation)._

## Overview

The Ably Kafka Connector is a sink connector used to publish data from [Apache Kafka](http://kafka.apache.org/) into [Ably](https://ably.com) and is available on [Confluent Hub](https://www.confluent.io/hub/ably/kafka-connect-ably). It has also been tested with [AWS MSK](https://aws.amazon.com/msk/).

The connector will publish data from one or more [Kafka topics](https://docs.confluent.io/platform/current/kafka/introduction.html#main-concepts-and-terminology) into one or more  [Ably channels](https://ably.com/documentation/core-features/channels).

The connector is built on top of [Apache Kafka Connect](http://docs.confluent.io/current/connect/) and can be run locally with Docker, installed into an instance of Confluent Platform or attached to an AWS MSK cluster through
[MSK Connect](https://aws.amazon.com/msk/features/msk-connect/).

## Install

Install the connector using the [Confluent Hub Client](#confluent-hub-installation) or [manually](#manual-installation) on Confluent Platform. Alternatively deploy it locally using [Docker](#docker). If installing in Amazon MSK see the AWS MSK section below.

### Confluent Hub installation

To install the connector on a local installation of Confluent using the Confluent Hub Client:

1. Ensure that the Confluent Hub Client is installed. See the [Confluent instructions](https://docs.confluent.io/home/connect/confluent-hub/client.html#installing-c-hub-client) for steps to complete this.

2. Run the following command to install the Ably Kafka Connector:

    `confluent-hub install ably/kafka-connect-ably:<version>`

    Where `<version>` is the latest version of the connector.

3. [Configure](#configuration) the connector.

### Manual installation

To manually install the connector on a local installation of Confluent:

1. Obtain the `.zip` of the connector from Confluent Hub or this repository:

    **From Confluent Hub**:

    Visit the [Ably Kafka Connector](https://www.confluent.io/hub/ably/kafka-connect-ably) page on Confluent Hub and click the **Download** button.

    **From this repository**:

    1. Clone the repository:

        `git clone git@github.com:ably/kafka-connect-ably.git`

    2. Build the connector using [Maven](https://maven.apache.org/):

        `mvn clean package`

    3. A `.zip` file will be produced in the `/target/components/packages/` folder after the process has run.

2. Extract the `.zip` into a directory specified in the `plugin.path` of your connect worker's configuration properties file. See the [Confluent instructions](https://docs.confluent.io/home/connect/install.html#install-connector-manually) for further information on this step.

3. [Configure](#configuration) the connector.


### Confluent Cloud Custom Connector

It is possible to use the connector as a plugin on Confluent Cloud as a [Custom Connector](https://docs.confluent.io/cloud/current/connectors/bring-your-connector/overview.html). These steps assume that you have created a Confluent Cloud account and configured your cluster.

> [!IMPORTANT]  
> In order to run Ably connector, your Kafka cluster must reside in a [supported cloud provider and region](https://docs.confluent.io/cloud/current/connectors/bring-your-connector/custom-connector-fands.html#cc-byoc-regions).

1. Obtain the `.zip` of the connector as per [the manual installation guide](#manual-installation).
2. Inside the cluster on your Confluent Cloud account, add a new Connector
3. Instead of selecting Ably Kafka Connector from the Hub, instead click Add Plugin
4. Give the plugin a name, and set the class to `com.ably.kafka.connect.ChannelSinkConnector`
5. Upload the `.zip` file you obtained in step 1
6. In the plugin config, insert the following, replacing the placeholder with your Ably API key:

```json
{
  "connector.class": "com.ably.kafka.connect.ChannelSinkConnector",
  "tasks.max": "3",
  "group.id": "ably-connect-cluster",
  "topics": "<topic1>,<topic2>",
  "client.id": "Ably-Kafka-Connector",
  "channel": "#{topic}",
  "message.name": "#{topic}_message",
  "client.key": "<YOUR_ABLY_API_KEY>",
  "key.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
  "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
  "value.converter.schemas.enable": "false"
}
```
7. When asked for an endpoint, enter `rest.ably.io:443:TCP`. If you are using Kafka Schema Registry, also add `<SCHEMA_REGISTRY_URL>:443:TCP`.

### AWS MSK

See the getting started instructions and example deployment in the [examples section](examples/msk_connect/README.md).

### Docker

There is a [`docker-compose.yml`](docker-compose.yml) file for standalone mode and an alternative [`docker-compose-distributed.yml`](docker-compose-distributed.yml) for distributed mode included in this repository that can be used to run the connector locally using [Docker Compose](https://docs.docker.com/compose/). The Docker Compose file is based on the [Confluent Platform Docker images](https://docs.confluent.io/platform/current/installation/docker/image-reference.html).

1. Create and configure a [configuration file](#configuration) ensuring you have set at least the basic properties.
**Note:** You must provide connector properties when starting connector in distributed mode. 
An example cURL command to start the connector in distributed mode is:


```shell
 curl -X POST -H "Content-Type: application/json" --data '{"name": "ably-channel-sink",
     "config": {"connector.class":"com.ably.kafka.connect.ChannelSinkConnector", "tasks.max":"3", 
     "group.id":"ably-connect-cluster",
     "topics":"topic1,topic2","client.id":"Ably-Kafka-Connector","channel":"#{topic}","message.name": "#{topic}_message",
     "client.key":"<Put your API key here>" }}' http://localhost:8083/connectors
```
2. Start the cluster using:

    `docker-compose up -d`
 for standalone mode or

   `docker-compose -f docker-compose-distributed.yml up -d` for distributed mode.
   
**Note**: You can view the logs using `docker-compose logs connector`

**Note 2**: You must start your connectors using [Connect REST interface](https://docs.confluent.io/platform/current/connect/references/restapi.html) when using distributed mode.


3. Once the containers have started, you can test the connector by subscribing to your Ably channel using [SSE](https://ably.com/documentation/sse) in a new terminal window. Replace `<channel-name>` with the channel set in your configuration file and `<ably-api-key>` with an API key with the capability to subscribe to the channel.

    `curl -s -u "<ably-api-key>" "https://realtime.ably.io/sse?channel=<channel-name>&v=1.1"`

    **Note**: SSE is only used as an example. An Ably SDK can also be used to subscribe to the channel.

4. Produce a set of test messages in Kafka using the Kafka CLI tool. Replace `<kafka-topic-name>` with one of the topics set in your configuration file.

    ```
    docker-compose exec -T kafka kafka-console-producer --topic <kafka-topic-name> --broker-list kafka:9092 <<EOF
    message 1
    message 2
    message 3
    EOF
    ```

5. In the terminal window where you subscribed to the Ably channel, you will receive messages similar to the following:

    ```
    id: e026fVvywAz6Il@1623496744539-0
    event: message
    data: {"id":"1543960661:0:0","clientId":"kafka-connect-ably-example","connectionId":"SuJTceISnT","timestamp":1623496744538,"encoding":"base64", "channel":"kafka-connect-ably-example","data":"bWVzc2FnZSAx","name":"sink"}

    id: e026fVvywAz6Il@1623496744539-1
    event: message
    data: {"id":"1543960661:0:1","clientId":"kafka-connect-ably-example","connectionId":"SuJTceISnT","timestamp":1623496744538,"encoding":"base64", "channel":"kafka-connect-ably-example","data":"bWVzc2FnZSAy","name":"sink"}

    id: e026fVvywAz6Il@1623496744539-2
    event: message
    data: {"id":"1543960661:0:2","clientId":"kafka-connect-ably-example","connectionId":"SuJTceISnT","timestamp":1623496744538,"encoding":"base64", "channel":"kafka-connect-ably-example","data":"bWVzc2FnZSAz","name":"sink"}
    ```
#### Publishing messages with a Push Notification

Messages can be delivered to end user devices as Push Notifications by setting a Kafka message header named `com.ably.extras.push` with a notification payload, for example:

```json
{
  "notification": {
    "title": "Notification title",
    "body": "This is the body of notification"
  },
  "data": {
    "foo": "foo",
    "bar": "bar"
  }
}
```

Extra Ably configuration is also required to enable push notifications, see the [Push Notification documentation](https://ably.com/docs/general/push).

#### Publishing messages with schema

The Ably Kafka Connector supports messages which contain schema information by converting them to JSON before publishing 
them to Ably. To check how to use schema registry and supported converters, see 
[Using Kafka Connect with Schema Registry](https://docs.confluent.io/platform/current/schema-registry/connect.html).
For example, if messages on the Kafka topic are serialized using Avro with schemas registered at https://<your-schema-registry-host>, 
then set the following configuration so that those messages are converted from Avro to JSON:
```
value.converter=io.confluent.connect.avro.AvroConverter
value.converter.schema.registry.url=https://<your-schema-registry-host>
```

If you're running the Ably Kafka Connector locally using Docker Compose as outlined above, then you can use the [`kafka-avro-console-producer` CLI](https://docs.confluent.io/platform/current/tutorials/examples/clients/docs/kafka-commands.html#produce-avro-records) to test producing Avro serialized messages by running the following:
```shell
 docker-compose exec -T schema-registry kafka-avro-console-producer \
   --topic topic1 \
   --broker-list kafka:9092 \
   --property key.schema='{"type":"string"}' \
   --property parse.key=true \
   --property key.separator=":" \
   --property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"count","type":"int"}]}' \
   --property schema.registry.url=http://schema-registry:8081 <<EOF
"key1":{"count":1}
EOF
```

You should receive following Ably message where you subscribed. You will also receive an Avro-formatted key base64 encoded in the extras. 

```json
{
	"clientId": "Ably-Kafka-Connector",
	"connectionId": "VSuDXysgaz",
	"data": {
		"count": 1
	},
	"extras": {
    "kafka": {
      "key": "AAAAAKEIa2V5MQ=="
    }
	},
	"id": "-868034334:0:351",
	"name": "topic1_message",
	"timestamp": 1653923422360
}
```

Note that configuring the schema registry and appropriate key or value converters also enables referencing of record
field data within [Dynamic Channel Configuration](#dynamic-channel-configuration)

## Breaking API Changes in Version 2.0.0

Please see our [Upgrade / Migration Guide](UPDATING.md) for notes on changes you need to make to your configuration to update it with changes introduced by version 2.0.0 of the connector.

## Configuration

Configuration is handled differently depending on how the connector is installed:

| Installation | Configuration |
| ------------ | ------------- | 
| Docker | Create a `docker-compose-connector.properties` file in the `/config` directory. An [example file](config/example-connector.properties) already exists. |
| Single connect worker | Provide a configuration file as a [command line argument](https://docs.confluent.io/home/connect/userguide.html#standalone-mode). |
| Distributed connect workers | Use the Confluent REST API [`/connectors` endpoint](https://docs.confluent.io/platform/current/connect/references/restapi.html#post--connectors) to pass the configuration as JSON. |

### Configuration properties

The basic properties that must be configured for the connector are:

| Property        | Description                                                                                                                                                                                                                                       | Type      | Default                                      |
|-----------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------|----------------------------------------------|
| channel         | The name of the [Ably channel](https://ably.com/documentation/realtime/channels) to publish to. See also: [Dynamic channel configuration](#Dynamic-Channel-Configuration)                                                                         | *String*  ||
| client.key      | An API key from your Ably dashboard to use for authentication. This must have the [publish capability](https://ably.com/documentation/core-features/authentication#capabilities-explained) for the `channel` being published to by the connector. | *String*  ||
| client.id       | The [Ably client ID](https://ably.com/documentation/realtime/authentication#identified-clients) to use for the connector.                                                                                                                         | *String*  | kafka-connect-ably-example                   |
| name            | A globally unique name for the connector.                                                                                                                                                                                                         | *String*  | ably-channel-sink                            |
| topics          | A comma separated list of Kafka topics to publish from.                                                                                                                                                                                           | *String*  |
| tasks.max       | The maximum number of tasks to use for the connector.                                                                                                                                                                                             | *Integer* | 1                                            |
| connector.class | The name of the class for the connector. This must be a subclass of `org.apache.kafka.connect.connector`.                                                                                                                                         | *String*  | `io.ably.kakfa.connect.ChannelSinkConnector` |

The advanced properties that can be configured for the connector are:

| Property                          | Description                                                                                                                                            | Type      | Default |
|-----------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|-----------|---------|
| message.name                      | Ably message name to publish. Can be a pattern, as per [Dynamic Channel Configuration](#dynamic-channel-configuration) below.                          | *String*  |         |
| batchExecutionThreadPoolSize      | The maximum number of parallel outgoing REST API requests to publish content to Ably                                                                   | *Integer* | 10      |
| batchExecutionMaxBufferSize       | The maximum number of records to publish in a single batch to Ably. The maximum size of these batches must be less than the [maximum batch publish size](https://ably.com/docs/api/rest-api#batch-publish). | *Integer* | 100     |
| batchExecutionMaxBufferSizeMs     | The maxmium amount of time (in milliseconds) to wait for the batch publishing buffer to fill before publishing anyway                                  | *Integer* | 100     |
| onFailedRecordMapping             | Action to take if a dyanmic channel mapping fails for a record. See [Dynamic Channel Configuration](#Dynamic-Channel-Configuration) for full details.  | *String*  | stop    |
| client.async.http.threadpool.size | The size of the asyncHttp threadpool.                                                                                                                  | *Integer* | 64      |
| client.fallback.hosts             | A list of custom fallback hosts. This will override the default fallback hosts.                                                                        | *List*    ||
| client.http.max.retry.count       | The maximum number of fallback hosts to use when an HTTP request to the primary host is unreachable or indicates that it is unserviceable.             | *Integer* | 3       |
| client.http.open.timeout          | The timeout period for opening an HTTP connection.                                                                                                     | *Integer* | 4000    |
| client.http.request.timeout       | The timeout period for any single HTTP request and response.                                                                                           | *Integer* | 15000   |
| client.proxy                      | Sets whether the configured proxy options are used.                                                                                                    | *Boolean* ||
| client.proxy.host                 | The proxy host to use. Requires `client.proxy` to be set to `true`.                                                                                    | *String*  ||
| client.proxy.non.proxy.hosts      | A list of hosts excluded from using the proxy. Requires `client.proxy` to be set to `true`.                                                            | *List*    ||
| client.proxy.username             | The client proxy username. Requires `client.proxy` to be set to `true`.                                                                                | *String*  ||
| client.proxy.password             | The client proxy password. Requires `client.proxy` to be set to `true`.                                                                                | *String*  ||
| client.proxy.port                 | The client proxy port. Requires `client.proxy` to be set to `true`.                                                                                    | *Integer* ||
| client.proxy.pref.auth.type       | The authentication type to use with the client proxy. Must be one of `BASIC`, `DIGEST` or `X_ABLY_TOKEN`. Requires `client.proxy` to be set to `true`. | *String*  | Basic   |
| client.push.full.wait             | Sets whether Ably should wait for all the effects of push REST requests before responding.                                                             | *Boolean* ||
| client.tls                        | Sets whether TLS is used for all connection types.                                                                                                     | *Boolean* | True    |
| client.loglevel                   | Sets the verbosity of logging.                                                                                                                         | *Integer* | 0       |
| client.environment                | Custom Ably environment (https://ably.com/docs/platform-customization)                                                                                 | *String*  ||

## Buffering Records

The Ably Kafka connector buffers records locally so that larger batches can be sent to Ably in parallel for improved
throughput. This behaviour is configurable using these settings:

* `batchExecutionMaxBufferSizeMs` - the maximum amount of time to wait (in milliseconds) for record data to accumulate 
  before submitting as many records as have been buffered so far to Ably.
* `batchExecutionMaxBufferSize` - the maximum number of records to buffer before submitting to Ably
* `batchExecutionThreadPoolSize` - the size of the thread pool used to submit buffered batches to Ably, and therefore
  the maximum number of concurrent submissions to Ably per sink task.

Some consideration should be given with respect to your workload and requirements:

* `batchExecutionMaxBufferSizeMs` is effectively the minimum latency for outgoing records. Increasing this value gives
  records more time to accumulate so that more can be sent in each batch to Ably for improved efficiency, but if your
  workload requires that latency between Kafka and the end user is low, you may need to decrease this value.
* `batchExecutionMaxBufferSize` can be used to set a maximum on the number of records being sent to Ably. You should
  consider your typical outgoing Ably message sizes and ensure that this is not set so high that batch submissions to
  Ably may exceed your account limits.

### Message Ordering

If your workload requires that messages are sent to Ably channels in the order that they were published to Kafka Topic
partitions, you will need to disable parallel submissions with sink tasks from the Ably connector as follows:

* Set `batchExecutionThreadPoolSize=1` to prevent parallel submissions per task
* Likely increase `max.tasks` to be the desired number of separate task instances required to achieve parallelism across
  all topic partitions


## Dynamic Channel Configuration

Ably [Channels](https://ably.com/docs/channels) are very lightweight, and it's idiomatic to use very high numbers of distinct
channels to send messages between users. In many use cases, it makes sense to create an Ably channel per user or per session,
meaning there could be millions in total. Contrast with a typical Kafka deployment, where you're more likely to be putting
records related to all users but of some common type through a single topic.

To enable "fan-out" to high numbers of channels from your Kafka topic, the Ably Kafka Connector supports Dynamic Channel
Configuration, whereby you configure a template string to substitute data from incoming Kafka records into the outgoing Ably
Channel name. The same functionality is also supported for the [Message](https://ably.com/docs/channels/messages) `name`
field, if required.

To make use of this feature, simply set the `channel` and/or `message.name` settings in your Connector properties file
to reference data from the incoming Kafka record key or value, or metadata from the Kafka Topic, using the `#{...}`
placeholder. Referencing data within a record key or value is only possible when making use of 
[Kafka Connector Schema Support](https://docs.confluent.io/platform/current/schema-registry/index.html), though Topic
metadata can always be referenced without a schema. References to unstructured `key` values is also supported, assuming
they can be converted to a string.

For example, the following configuration references a field within the record value and the topic name:

```properties
channel = user:#{value.userId}
message.name = #{topic.name}
```

This assumes that a schema registry is available and contains a schema for record values, and that an appropriate
converter has been configured for values. If the value schema contains a field, `userId`, the configuration above
will substitute those values into the outgoing channel names as per the template. The Ably Message name will also
be set to the Kafka topic name, in this example.

Fields can be nested within other `Structs`, for example `value.someStruct.userId` would also be valid if `someStruct`
has `STRUCT` type and `userId` can be converted to a string. Given that both message and channel names are ultimately
strings, the referenced fields must be reasonably interpretable as a string. The supported conversions are:

* String
* Integer (any precision)
* Boolean
* Bytes (assumed to be UTF-8 encoded string data)

The table below summarises the substitutions that can be made within a `#{}` placeholder:

| Placeholder            | Description                                                  |
|------------------------|--------------------------------------------------------------|
| `#{topic.name}`        | The Kafka Topic name                                         |
| `#{topic.partition}`   | The Kafka Topic partition                                    |
| `#{topic}`             | Alias for `topic.name`                                       |
| `#{key}`               | The record key. Must be convertible to a string              |
| `#{key.nestedField}`   | If key is a struct with a schema, uses `nestedField` value   |
| `#{value.nestedField}` | If value is a struct with a schema, uses `nestedField` value |

### Handling Failed Mappings

Dynamic channel mapping can fail at runtime, if:

* The template references a field that a record is missing
* The referenced field cannot be converted to a string

In these situations, it's possible to configure the desired error handling behaviour by setting the `onFailedRecordMapping`
property to one of the following values:

* `stop` (default) - Treat failed mappings as fatal and stop the Sink Task completely.
* `skip` - Silently ignore records that cannot be mapped to the required template. Use this only if you're sure that
the Kafka topic contains irrelevant records you'd like to filter out this way.
* `dlq` - Send records with failed mappings along with the error (as a header) to a configured 
[dead-letter queue](#dead-letter-queues).

## Dead Letter Queues

The Ably Kafka Connector is able to forward records to a dead-letter queue topic using Kafka Connect dead-letter queue
support. You can learn more about dead-letter queues [here](https://www.confluent.io/en-gb/blog/kafka-connect-deep-dive-error-handling-dead-letter-queues/).

As an example, adding the following configuration to your connector properties file will cause all failed records
to be sent to a `dlq_ably_sink` topic with a replication factor of 1 and headers attached giving you full exception
details for each record.

```properties
errors.tolerance = all
errors.deadletterqueue.topic.name = dlq_ably_sink
errors.deadletterqueue.topic.replication.factor=1
errors.deadletterqueue.context.headers.enable=true
```

Situations in which the Ably connector will forward records to a dead-letter queue include:

* Errors submitting to Ably, perhaps due to insufficient permissions to publish to a specific destination channel
* Serious Ably service outages, after retrying with fallback endpoints
* Failed dynamic channel mappings, if `onFailedRecordMapping = dlq`

## Contributing

For guidance on how to contribute to this project, see [CONTRIBUTING.md](CONTRIBUTING.md).
