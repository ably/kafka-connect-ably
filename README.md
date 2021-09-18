# Ably Kafka Connector

_[Ably](https://ably.com) is the platform that powers synchronized digital experiences in realtime. Whether attending an event in a virtual venue, receiving realtime financial information, or monitoring live car performance data – consumers simply expect realtime digital experiences as standard. Ably provides a suite of APIs to build, extend, and deliver powerful digital experiences in realtime for more than 250 million devices across 80 countries each month. Organizations like Bloomberg, HubSpot, Verizon, and Hopin depend on Ably’s platform to offload the growing complexity of business-critical realtime data synchronization at global scale. For more information, see the [Ably documentation](https://ably.com/documentation)._

## Overview

The Ably Kafka Connector is a sink connector used to publish data from [Apache Kafka](http://kafka.apache.org/) into [Ably](https://ably.com).

The connector will publish data from one or more [Kafka topics](https://docs.confluent.io/platform/current/kafka/introduction.html#main-concepts-and-terminology) into a single [Ably channel](https://ably.com/documentation/core-features/channels).

The connector is built on top of [Apache Kafka Connect](http://docs.confluent.io/current/connect/) and can be run locally with Docker, or installed into an instance of Confluent Platform.

## Install

The connector can be installed on [Confluent Platform](#confluent-platform) or deployed locally using [Docker](#docker).

### Confluent Platform

To install the connector on a local installation of Confluent:

1. Clone this Github repository:

    `git clone git@github.com:ably/kafka-connect-ably.git`

2. Build the connector using [Maven](https://maven.apache.org/):

    `mvn clean package`

3. A `.zip` file will be produced in the `/target/components/packages/` folder after the process has run.

4. Install the `.zip` into a directory specified in the `plugin.path` of your connect worker's configuration properties file. See the [Confluent instructions](https://docs.confluent.io/home/connect/install.html#install-connector-manually) for further information on this step.

5. [Configure](#configuration) the connector.

### Docker

There is a [`docker-compose.yml`](docker-compose.yml) file included in this repository that can be used to run the connector locally using [Docker Compose](https://docs.docker.com/compose/). The Docker Compose file is based on the [Confluent Platform Docker images](https://docs.confluent.io/platform/current/installation/docker/image-reference.html).

1. Create and configure a [configuration file](#configuration) ensuring you have set at least the basic properties.

2. Start the cluster using:

    `docker-compose up -d`

    **Note**: You can view the logs using `docker-compose logs connector`

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

## Configuration

Configuration is handled differently depending on how the connector is installed:

| Installation | Configuration |
| ------------ | ------------- | 
| Docker | Create a `docker-compose-connector.properties` file in the `/config` directory. An [example file](config/example-connector.properties) already exists. |
| Single connect worker | Provide a configuration file as a [command line argument](https://docs.confluent.io/home/connect/userguide.html#standalone-mode). |
| Distributed connect workers | Use the Confluent REST API [`/connectors` endpoint](https://docs.confluent.io/platform/current/connect/references/restapi.html#post--connectors) to pass the configuration as JSON. |

### Configuration properties

The basic properties that must be configured for the connector are:

| Property | Description | Type | Default |
| -------- | ----------- | ---- | ------- |
| channel | The name of the [Ably channel](https://ably.com/documentation/realtime/channels) to publish to. | *String* ||
| client.key | An API key from your Ably dashboard to use for authentication. This must have the [publish capability](https://ably.com/documentation/core-features/authentication#capabilities-explained) for the `channel` being published to by the connector. | *String* ||
| client.id | The [Ably client ID](https://ably.com/documentation/realtime/authentication#identified-clients) to use for the connector. | *String* | kafka-connect-ably-example |
| name | A globally unique name for the connector. | *String* | ably-channel-sink |
| topics | A comma separated list of Kafka topics to publish from. | *String* |
| tasks.max | The maximum number of tasks to use for the connector. | *Integer* | 1 |
| connector.class | The name of the class for the connector. This must be a subclass of `org.apache.kafka.connect.connector`. | *String* | `io.ably.kakfa.connect.ChannelSinkConnector` |

The advanced properties that can be configured for the connector are:

| Property | Description | Type | Default |
| -------- | ----------- | ---- | ------- |
| client.async.http.threadpool.size | The size of the asyncHttp threadpool. | *Integer* | 64 |
| client.auto.connect | Sets whether the initiation of a connection when the library is instanced is automatic or not. | *Boolean* | True |
| client.channel.cipher.key | Sets whether encryption is enforced for the channel when not null. Also specifies encryption-related parameters such as algorithm, chaining mode, key length and key. | *String* ||
| client.channel.params | Specify additional channel parameters in the format `key1=value1,key2=value2`. | *List* ||
| client.channel.retry.timeout | The timeout period for [retry attempts for attaching to a channel](https://ably.com/documentation/client-lib-development-guide/features#RTL13b). | *Integer* | 15000 |
| client.echo.messages | Sets whether messages originating from this connection are echoed back on the same connection. | *Boolean* | True |
| client.fallback.hosts | A list of custom fallback hosts. This will override the default fallback hosts. | *List* ||
| client.http.max.retry.count | The maximum number of fallback hosts to use when an HTTP request to the primary host is unreachable or indicates that it is unserviceable. | *Integer* | 3 |
| client.http.open.timeout | The timeout period for opening an HTTP connection. | *Integer* | 4000 |
| client.http.request.timeout | The timeout period for any single HTTP request and response. | *Integer* | 15000 |
| client.idempotent.rest | Sets whether idempotent REST publishing is used. | *Boolean* | True |
| client.proxy | Sets whether the configured proxy options are used. | *Boolean* ||
| client.proxy.host | The proxy host to use. Requires `client.proxy` to be set to `true`. | *String* ||
| client.proxy.non.proxy.hosts | A list of hosts excluded from using the proxy. Requires `client.proxy` to be set to `true`. | *List* ||
| client.proxy.username | The client proxy username. Requires `client.proxy` to be set to `true`. | *String* ||
| client.proxy.password | The client proxy password. Requires `client.proxy` to be set to `true`. | *String* ||
| client.proxy.port | The client proxy port. Requires `client.proxy` to be set to `true`. | *Integer* ||
| client.proxy.pref.auth.type | The authentication type to use with the client proxy. Must be one of `BASIC`, `DIGEST` or `X_ABLY_TOKEN`. Requires `client.proxy` to be set to `true`. | *String* | Basic |
| client.push.full.wait | Sets whether Ably should wait for all the effects of push REST requests before responding. | *Boolean* ||
| client.queue.messages | Sets whether the default queueing of messages when connection states that anticipate an imminent connection (connecting and disconnected) are suppressed or not. If set to `false`, publish and presence state changes will fail immediately if not in the connected state. | *Boolean* | True |
| client.realtime.request.timeout | The timeout period before a realtime client library establishing a connection with Ably, or sending a `HEARTBEAT`, `CONNECT`, `ATTACH`, `DETACH` or `CLOSE` `ProtocolMessage` to Ably, will consider that request as failed and trigger a suitable failure condition. | *Long* | 10000 |
| client.recover | A connection recovery string, specified by a client when initializing the library with the intention of inheriting the state of an earlier connection. | *String* ||
| client.tls | Sets whether TLS is used for all connection types. | *Boolean* | True |
| client.token.params | Sets whether the configured token parameters are used. | *Boolean* ||
| client.token.params.capability | Stringified JSON capability requirements for the token. When omitted, the REST API default to allow all operations is applied by Ably, with the string value `{“*”:[“*”]}`. Requires `client.token.params` to be set to `true`. | *String* ||
| client.token.params.client.id | The client ID to include with the token. Requires `client.token.params` to be set to `true`. | *String* ||
| client.token.params.ttl | The requested time to live (TTL) for the token in milliseconds. When omitted, the REST API default of 60 minutes is applied by Ably. Requires `client.token.params` to be set to `true`. | *Boolean* | 0 |
| client.transport.params | Any additional parameters to be sent in the query string when initiating a realtime connection in the format `key1=value1,key2=value` without URL encoding. | *List* ||
| client.use.binary.protocol | Set to `false` to force the library to use JSON encoding for REST and realtime operations, instead of msgpack encoding. | *Boolean* | True |
| client.loglevel | Sets the verbosity of logging. | *Integer* | 0 |
