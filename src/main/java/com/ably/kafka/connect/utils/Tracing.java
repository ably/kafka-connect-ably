package com.ably.kafka.connect.utils;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;
import io.opentelemetry.contrib.aws.resource.Ec2Resource;
import io.opentelemetry.contrib.awsxray.AwsXrayIdGenerator;
import io.opentelemetry.contrib.awsxray.propagator.AwsXrayPropagator;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.semconv.ResourceAttributes;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Tracing {
    private static final Logger logger = LoggerFactory.getLogger(OpenTelemetry.class);

    private static OpenTelemetry buildOpenTelemetry() {
        logger.info("initialising OpenTelemetry");

        final String exporter = System.getenv().getOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317");

        Resource resource = Resource.getDefault()
            .merge(Ec2Resource.get())
            .merge(Resource.create(Attributes.builder()
                .put(ResourceAttributes.SERVICE_NAME, "ably-kafka-connector")
                .build()));

        return OpenTelemetrySdk.builder()
                .setPropagators(
                        ContextPropagators.create(
                                TextMapPropagator.composite(
                                        W3CTraceContextPropagator.getInstance(),
                                        AwsXrayPropagator.getInstance())))
                .setTracerProvider(
                        SdkTracerProvider.builder()
                                .addSpanProcessor(
                                        BatchSpanProcessor.builder(OtlpGrpcSpanExporter.builder().setEndpoint(exporter).build()).build())
                                .setIdGenerator(AwsXrayIdGenerator.getInstance())
                                .setResource(resource)
                                .build())
                .buildAndRegisterGlobal();
    }

    public static final OpenTelemetry otel = buildOpenTelemetry();

    public static final Tracer tracer = otel.getTracer("ably-kafka-connector");

    // Implement TextMapGetter to extract telemetry context from Kafka message
    // headers
    public static final TextMapGetter<SinkRecord> textMapGetter =
        new TextMapGetter<>() {
            @Override
            public String get(SinkRecord carrier, String key) {
                Header header = carrier.headers().lastWithName(key);
                if (header == null) {
                  return null;
                }
                return String.valueOf(header.value());
            }

            @Override
            public Iterable<String> keys(SinkRecord carrier) {
                return StreamSupport.stream(carrier.headers().spliterator(), false)
                    .map(Header::key)
                    .collect(Collectors.toList());
            }
        };

    // Implement TextMapSetter to inject telemetry context into Kafka message
    // headers
    public static final TextMapSetter<SinkRecord> textMapSetter =
        new TextMapSetter<>() {
            @Override
            public void set(SinkRecord record, String key, String value) {
                  record.headers().remove(key).addString(key, value);
            }
        };
}
