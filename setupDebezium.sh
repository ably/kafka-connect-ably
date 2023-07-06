#!/bin/bash

# Source configuration
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
# What Kafka-connect instance to setup Debezium into
CONNECT_HOST="127.0.0.1"
CONNECT_PORT="8083"
# Name of this connector instance
CONNECTOR_NAME="debezium-connector"

# URL of the connect instance
CONNECT_URL="http://${CONNECT_HOST}:${CONNECT_PORT}"
# URL of the connectors management
CONNECTORS_MANAGEMENT_URL="http://${CONNECT_HOST}:${CONNECT_PORT}/connectors"
# URL of this particular conneector instance
CONNECTOR_URL="${CONNECTORS_MANAGEMENT_URL}/${CONNECTOR_NAME}"
# Debezium parameters. Check
# https://debezium.io/documentation/reference/stable/connectors/mysql.html#_required_debezium_mysql_connector_configuration_properties
# for the full list of available properties

MYSQL_HOST="mysql-master"
MYSQL_PORT="3306"
MYSQL_USER="root"
MYSQL_PASSWORD="root"
# Comma-separated list of regular expressions that match the databases for which to capture changes
MYSQL_DBS="sbtest"
# Comma-separated list of regular expressions that match fully-qualified table identifiers of tables
MYSQL_TABLES="sbtest1"
#KAFKA_BOOTSTRAP_SERVERS="one-node-cluster-0.one-node-cluster.redpanda.svc.cluster.local:9092"
KAFKA_BOOTSTRAP_SERVERS="kafka:9092"
KAFKA_TOPIC="schema-changes.sbtest"

# Connector joins the MySQL database cluster as another server (with this unique ID) so it can read the binlog.
# By default, a random number between 5400 and 6400 is generated, though the recommendation is to explicitly set a value.
DATABASE_SERVER_ID="5432"
# Unique across all other connectors, used as a prefix for Kafka topic names for events emitted by this connector.
# Alphanumeric characters, hyphens, dots and underscores only.
DATABASE_SERVER_NAME="SERVER5432"

#https://debezium.io/documentation/reference/stable/configuration/avro.html
cat <<EOF | curl --request POST --url "${CONNECTORS_MANAGEMENT_URL}" --header 'Content-Type: application/json' --data @-
      {
        "name": "${CONNECTOR_NAME}",
        "config": {
          "connector.class": "io.debezium.connector.mysql.MySqlConnector",
          "tasks.max": "1",
          "snapshot.mode": "initial",
          "snapshot.locking.mode": "minimal",
          "snapshot.delay.ms": 10000,
          "include.schema.changes":"true",
          "include.schema.comments": "true",
          "database.hostname": "${MYSQL_HOST}",
          "database.port": "${MYSQL_PORT}",
          "database.user": "${MYSQL_USER}",
          "database.password": "${MYSQL_PASSWORD}",
          "database.server.id": "${DATABASE_SERVER_ID}",
          "database.server.name": "${DATABASE_SERVER_NAME}",
          "database.whitelist": "${MYSQL_DBS}",
          "database.allowPublicKeyRetrieval":"true",
          "database.history.kafka.bootstrap.servers": "${KAFKA_BOOTSTRAP_SERVERS}",
          "database.history.kafka.topic": "${KAFKA_TOPIC}",

          "key.converter": "io.confluent.connect.avro.AvroConverter",
          "value.converter": "io.confluent.connect.avro.AvroConverter",

          "key.converter.schema.registry.url": "http://schema-registry:8081",
          "value.converter.schema.registry.url":"http://schema-registry:8081",

          "topic.creation.$alias.partitions": 6,
          "topic.creation.default.replication.factor": 1,
          "topic.creation.default.partitions": 6,

          "provide.transaction.metadata": "true",
          "max.batch.size": 128000,
          "max.queue.size": 512000
        }
      }
EOF
