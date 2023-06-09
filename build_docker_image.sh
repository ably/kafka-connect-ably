#!/bin/bash

mvn clean package -DskipTests=true
docker build -f Dockerfile_development . -t connector