package com.ably.kafka.connect.utils;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Objects;

/**
 * Pairing of records to errors for capturing objects sent to the DLQ
 */
public class CapturedDlqError {
    public final SinkRecord record;
    public final Throwable error;

    public CapturedDlqError(SinkRecord record, Throwable error) {
        this.record = record;
        this.error = error;
    }

    public SinkRecord getRecord() {
        return record;
    }

    public Throwable getError() {
        return error;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CapturedDlqError that = (CapturedDlqError) o;
        return Objects.equals(record, that.record) && Objects.equals(error, that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(record, error);
    }

    @Override
    public String toString() {
        return "CapturedDlqError{" +
            "record=" + record +
            ", error=" + error +
            '}';
    }
}
