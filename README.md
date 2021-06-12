# Kafka Connect Ably Connector

This is a sink connector for publishing data from [Apache Kafka](http://kafka.apache.org/)
into [Ably](https://ably.com), built on top of [Apache Kafka Connect](http://docs.confluent.io/current/connect/).

# Status

This connector is under heavy development and is not currently considered production ready.

# Installation

Build the connector using the standard Maven build lifecycle:

```
mvn clean package
```

This produces a ZIP file into the `target/components/packages` directory which can be manually installed on
your Connect workers by following [these instructions](https://docs.confluent.io/home/connect/install.html#install-connector-manually).

# Configuration

The connector needs to be configured with your Ably API key and the Ably channel
to publish messages to.

An example configuration file can be found in [`config/example-connector.properties`](config/example-connector.properties).

# Running locally

The easiest way to run the connector locally is using [Docker Compose](https://docs.docker.com/compose/).

This repository includes a [docker-compose.yml](docker-compose.yml) file which uses the
[Confluent Platform Docker images](https://docs.confluent.io/platform/current/installation/docker/image-reference.html).

To start the Docker Compose cluster, first copy the example properties file to
`config/docker-compose-connector.properties` and set `client.key` to your Ably
API key:

```
cp config/example-connector.properties config/docker-compose-connector.properties
```

Then start the cluster by running:

```
docker-compose up -d
```

You can check the logs of the connector with:

```
docker-compose logs connector
```

To check the connector is working, subscribe to messages on the `kafka-connect-ably-example`
Ably channel in a separate terminal window (this uses [SSE](https://ably.com/documentation/sse)
for convenience, but you could also use an [Ably SDK](https://ably.com/download)):

```
curl -s -u "<your Ably API key>" "https://realtime.ably.io/sse?channel=kafka-connect-ably-example&v=1.1"
```

Produce some messages to the `kafka-connect-ably-example` Kafka topic using the
`kafka-console-producer` CLI tool in the Kafka container:

```
docker-compose exec -T kafka kafka-console-producer --topic kafka-connect-ably-example --broker-list kafka:9092 <<EOF
message 1
message 2
message 3
EOF
```

The messages should have been published to Ably by the connector and appear on
the SSE subscription in your other terminal window:

```
id: e026fVvywAz6Il@1623496744539-0
event: message
data: {"id":"1543960661:0:0","clientId":"kafka-connect-ably-example","connectionId":"SuJTceISnT","timestamp":1623496744538,"encoding":"base64","channel":"kafka-connect-ably-example","data":"bWVzc2FnZSAx","name":"sink"}

id: e026fVvywAz6Il@1623496744539-1
event: message
data: {"id":"1543960661:0:1","clientId":"kafka-connect-ably-example","connectionId":"SuJTceISnT","timestamp":1623496744538,"encoding":"base64","channel":"kafka-connect-ably-example","data":"bWVzc2FnZSAy","name":"sink"}

id: e026fVvywAz6Il@1623496744539-2
event: message
data: {"id":"1543960661:0:2","clientId":"kafka-connect-ably-example","connectionId":"SuJTceISnT","timestamp":1623496744538,"encoding":"base64","channel":"kafka-connect-ably-example","data":"bWVzc2FnZSAz","name":"sink"}
```
