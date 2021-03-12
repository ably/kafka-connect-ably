# Introduction


kafka-connect-ably is a is a [Kafka Connector](http://kafka.apache.org/documentation.html#connect)
for publishing data from Kafka into Ably.

# Running in development


The [docker-compose.yml](docker-compose.yml) that is included in this repository is based on the Confluent Platform Docker images.
Take a look at the [quickstart](http://docs.confluent.io/current/cp-docker-images/docs/quickstart.html#getting-started-with-docker-client)
for the Docker images.


The docker compose setup provides a named docker network to ease conflicts with open ports on the host machine.

You can start the cluster by running:

```
docker-compose up -d
```

Ensusre that the kafka connector is built locally

```
mvn clean package
```

Start the connector in standalone mode.

```
bash ./bin/debug.sh
```

OR

Start the connector with debugging enabled (enabled debugger on port 5055)

```
export SUSPEND='y'
./bin/debug.sh
```
