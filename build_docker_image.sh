#!/bin/bash

./gradlew clean mskArchive
docker build -f Dockerfile_development . -t connector
