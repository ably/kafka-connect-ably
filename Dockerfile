FROM gradle:8.12-jdk17 AS build

WORKDIR /usr/src/app

# Copy Gradle configuration files
COPY build.gradle.kts settings.gradle.kts ./

# Download dependencies (cached layer)
RUN gradle dependencies --no-daemon || true

# Copy source code
COPY src src/
COPY config config/
COPY logos logos/
COPY README.md LICENSE ./

# Build the project
RUN gradle clean mskArchive --no-daemon

# Extract the MSK archive to get all JARs
RUN unzip build/distributions/msk/kafka-connect-ably-*-bin.zip -d /tmp/plugin && \
    mkdir -p /plugins && \
    cp /tmp/plugin/*.jar /plugins/

FROM confluentinc/cp-kafka-connect:7.9.2-1-ubi8

COPY --from=build /plugins/ /plugins/

ADD bin/docker-entrypoint.sh /bin/docker-entrypoint.sh
ENTRYPOINT ["/bin/docker-entrypoint.sh"]
