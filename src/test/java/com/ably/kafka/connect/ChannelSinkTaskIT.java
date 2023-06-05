package com.ably.kafka.connect;

import com.jayway.jsonpath.JsonPath;
import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.stream.Stream;

public class ChannelSinkTaskIT {

    private static Network network = Network.newNetwork();

    private static KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.2.0"))
            .withNetwork(network).withExposedPorts(9092).withNetworkAliases("kafka");

    public static DebeziumContainer debeziumContainer = DebeziumContainer.latestStable()
            .withNetwork(network)
            .withKafka(kafkaContainer)
            .dependsOn(kafkaContainer);


    public static PostgreSQLContainer<?> postgresContainer = new PostgreSQLContainer<>(DockerImageName.parse
            ("quay.io/debezium/postgres:15").asCompatibleSubstituteFor("postgres"))
            .withNetwork(network)
            .withNetworkAliases("postgres");

    public static GenericContainer ablySinkConnectContainer = new GenericContainer("connector:latest");

    @BeforeAll
    public static void startContainers() {
       // createConnectorJar();

        Startables.deepStart(Stream.of(
                        kafkaContainer, ablySinkConnectContainer, postgresContainer, debeziumContainer))
                .join();
    }

    private ConnectorConfiguration getConfiguration(int id) {
        // host, database, user etc. are obtained from the container
        return ConnectorConfiguration.forJdbcContainer(postgresContainer)
                .with("topic.prefix", "dbserver" + id)
                .with("slot.name", "debezium_" + id);
    }

    private String executeHttpRequest(String url) throws IOException {
        final OkHttpClient client = new OkHttpClient();
        final Request request = new Request.Builder().url(url).build();

        try (Response response = client.newCall(request).execute()) {
            return response.body().string();
        }
    }

    @Test
    public void messagesShouldBeReadInOrder() {

        // Register the debezium connector
        debeziumContainer.registerConnector("my-connector-1", getConfiguration(1));

        // task initialization happens asynchronously, so we might have to retry until the task is RUNNING
        Awaitility.await()
                .pollInterval(Duration.ofMillis(250))
                .atMost(Duration.ofSeconds(30))
                .untilAsserted(
                        () -> {
                            String status = executeHttpRequest(debeziumContainer.getConnectorStatusUri("my-connector-1"));

                            assertEquals(JsonPath.<String> read(status, "$.name"), "my-connector-1");
                            assertEquals(JsonPath.<String> read(status, "$.connector.state"), "RUNNING");
                            assertEquals(JsonPath.<String> read(status, "$.tasks[0].state"), "RUNNING");
                        }
                );

    }


    @AfterAll
    public static void stopContainers() {
        try {
            if (postgresContainer != null) {
                postgresContainer.stop();
            }
            if (kafkaContainer != null) {
                kafkaContainer.stop();
            }
            if (debeziumContainer != null) {
                debeziumContainer.stop();
            }
        }
        catch (Exception e) {
            // ignored
        }
    }
}