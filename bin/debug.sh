#!/usr/bin/env bash

: "${SUSPEND:=n}"

set -e

KAFKA_JMX_OPTS="-Xdebug -agentlib:jdwp=transport=dt_socket,server=y,suspend=${SUSPEND},address=*:5005"
DOCKER_IMAGE="confluentinc/cp-kafka-connect:6.0.2-1-ubi8"

docker run --rm --network="kafka-connect-ably_kafka_network"\
    -p "5005:5005" \
    -v "$(pwd)/config:/config" \
    -v "$(pwd)/build/distributions/msk:/plugins" \
    -e KAFKA_JMX_OPTS="${KAFKA_JMX_OPTS}" \
    "${DOCKER_IMAGE}" \
    /bin/connect-standalone /config/connect-avro-docker.properties /config/ChannelSinkConnector.properties
