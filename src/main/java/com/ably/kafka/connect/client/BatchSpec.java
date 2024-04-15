package com.ably.kafka.connect.client;

import io.ably.lib.types.Message;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Ably BatchSpec type for submission using the Ably Rest API.
 * GSON will serialise this to the expected JSON format.
 */
final public class BatchSpec {
    private final Set<String> channels;
    private final List<Message> messages;

    public BatchSpec(Set<String> channels, List<Message> messages) {
        this.channels = channels;
        this.messages = messages;
    }
    public BatchSpec(String channelName, Message message) {
        this(Collections.singleton(channelName), Collections.singletonList(message));
    }
    public Set<String> getChannels() {
        return channels;
    }
    public List<Message> getMessages() {
        return messages;
    }
}
