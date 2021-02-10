#!/usr/bin/env bash

: ${SUSPEND:='n'}

set -e


export KAFKA_JMX_OPTS="-Xdebug -agentlib:jdwp=transport=dt_socket,server=y,suspend=${SUSPEND},address=5005"

connect-standalone config/connect-avro-docker.properties config/ChannelSinkConnector.properties
