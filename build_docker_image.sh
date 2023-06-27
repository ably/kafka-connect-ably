#!/bin/bash

mvn clean package -DskipTests=true -Dmaven.javadoc.skip=true
docker build -f Dockerfile_development . -t connector
