FROM maven:3-openjdk-16 AS build

WORKDIR /usr/src/app

COPY pom.xml .

RUN mvn -DskipTests -Dassembly.skipAssembly=true clean install

COPY src src/

RUN mvn -DskipTests package

FROM confluentinc/cp-kafka-connect:7.4.0-2-ubi8

COPY --from=build /usr/src/app/target/kafka-connect-ably-msk-plugin/kafka-connect-target /plugins/

ADD bin/docker-entrypoint.sh /bin/docker-entrypoint.sh
ENTRYPOINT ["/bin/docker-entrypoint.sh"]
