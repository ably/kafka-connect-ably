#!/usr/bin/env bash

: "${PORT:=5005}"
: "${SUSPEND:=n}"

export KAFKA_JMX_OPTS="-Xdebug -agentlib:jdwp=transport=dt_socket,server=y,suspend=${SUSPEND},address=*:${PORT}"

exec "$@"
