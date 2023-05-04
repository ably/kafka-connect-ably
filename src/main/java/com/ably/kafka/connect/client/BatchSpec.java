package com.ably.kafka.connect.client;

import io.ably.lib.types.Message;

import java.util.List;
import java.util.Objects;
import java.util.Set;

public class BatchSpec {
    private final Set<String> channels;
    private final List<Message> messages;
    BatchSpec(Set<String> channels, List<Message> messages) {
        this.channels = channels;
        this.messages = messages;
    }
    public Set<String> getChannels() {
        return channels;
    }
    public List<Message> getMessages() {
        return messages;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BatchSpec batchSpec = (BatchSpec) o;
        return Objects.equals(channels, batchSpec.channels)
                && Objects.equals(messages, batchSpec.messages);
    }
    @Override
    public int hashCode() {
        return Objects.hash(channels, messages);
    }
}
