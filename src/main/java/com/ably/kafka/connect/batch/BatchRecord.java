package com.ably.kafka.connect.batch;

import org.apache.kafka.connect.sink.SinkRecord;
import io.opentelemetry.api.trace.Span;

public class BatchRecord {
    public final SinkRecord record;
    public final Span serverSpan;
    public final Span queueSpan;

    public BatchRecord(SinkRecord record, Span serverSpan, Span queueSpan) {
        this.record = record;
        this.serverSpan = serverSpan;
        this.queueSpan = queueSpan;
    }
}
