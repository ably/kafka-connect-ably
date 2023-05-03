//package com.ably.kafka.connect.integration;
//
//import org.apache.kafka.clients.admin.AdminClient;
//import org.apache.kafka.clients.admin.AdminClientConfig;
//import org.junit.BeforeClass;
//import org.junit.jupiter.api.Test;
//import org.testcontainers.Testcontainers;
//import org.testcontainers.containers.KafkaContainer;
//import org.testcontainers.containers.Network;
//import org.testcontainers.lifecycle.Startables;
//import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
//
//import java.util.stream.Stream;
//

// https://github.com/aiven/http-connector-for-apache-kafka/blob/main/src/integration-test/java/io/aiven/kafka/connect/http/IntegrationTest.java

//public class ChannelSinkConnectorIT {
//    private static Network network = Network.newNetwork();
//
//    private static KafkaContainer kafkaContainer = new KafkaContainer()
//            .withNetwork(network);
//
//    public static DebeziumContainer debeziumContainer =
//            new DebeziumContainer("quay.io/debezium/connect:2.2.0.Final")
//                    .withNetwork(network)
//                    .withKafka(kafkaContainer)
//                    .dependsOn(kafkaContainer);
//
//    @BeforeClass
//    public static void startContainers() {
//        Startables.deepStart(Stream.of(
//                        kafkaContainer, postgresContainer, debeziumContainer))
//                .join();
//    }
//
//    @Test
//    public void testWithHostExposedPort() throws Exception {
//        Testcontainers.exposeHostPorts(12345);
//        try (KafkaContainer kafka = new KafkaContainer(KAFKA_TEST_IMAGE)) {
//            kafka.start();
//            testKafkaFunctionality(kafka.getBootstrapServers());
//        }
//    }
//
//    private static void sendMessageToKafka() {
//        AdminClient adminClient = AdminClient.create(
//                ImmutableMap.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
//        );
//        KafkaProducer<String, String> producer = new KafkaProducer<>(
//                ImmutableMap.of(
//                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
//                        bootstrapServers,
//                        ProducerConfig.CLIENT_ID_CONFIG,
//                        UUID.randomUUID().toString()
//                ),
//                new StringSerializer(),
//                new StringSerializer()
//        );
//    }
//}
